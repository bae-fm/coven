# IO and observable work

This is the verification contract for [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), and the file calls in [E8](../api.md#e8-files-and-the-cache).
It is a specification of required checks, not an executable Lean proof.

## Requests and retained observations

Count provider requests, metadata and body bytes, lookup and redirect calls,
upload parts, retries, deletes and access requests separately. A provider
adapter must expose the actual calls behind a logical operation. Initialization,
eviction and interrupted transfers each have their own counts.

Ana has 20,000 old photos; Household has her phone, Ben's laptop and Carol's
tablet. With no changes or due work, a warm pass makes two listings and nine
next-number misses: **11 requests** when both listings fit one page, on S3,
Drive, Dropbox, OneDrive and CloudKit. It reads no bodies, rewrites no
positions and commits no database changes. The general count is the pages
for store-log device folders and positions, plus three misses per device.
Increasing unchanged history or file count changes none of those counts.
Increasing devices can add misses and pages; all native calls remain visible.

Drive's unknown-name miss is one exact parent/name query with no download.
A hit adds the download by id. Lookup pages, redirects, uncached folder ids
and transfer retries remain charged; the 11-request example assumes warm
folder knowledge and complete one-page query results. With a transfer limit
of one, Ben receiving one S3 note adds its GET and a positions PUT: 13 requests.
Higher fetch-ahead limits can add probes already in flight beyond the first
miss; they are counted too.

Checked headers, prefixes, decoded entries, peer revisions and file-reference
facts survive passes and reopen. A missing key cannot cause the same bytes
to be downloaded again. A failed transfer can retry its missing suffix;
all repeated bytes are charged. A refusal cannot be cleared by evicting its
bytes. A provider cooldown applies to every worker in its scope and survives
restart through the stored deadline and wait duration.

## Discovery obligations

Exercise the real reader/provider boundary with these histories. These are
required checks, not claims that an existing Lean model proves the new
discovery mechanism:

- Each writer publishes entries, writes and key copies in contiguous number
  order. A hit permits bounded parallel fetch-ahead; a miss stops scheduling
  beyond it. A failed read does not become a miss, and retained input advances
  the download position atomically. Applied positions still wait on causes.
- Cancelling an operation or changing membership cannot remove a numbered
  key copy from the middle of its publication sequence. Needs are checked
  before number reservation; numbered copies retain their bytes and publish.
  Whole-copy discovery reads the clear audience, key id and recipient;
  recipients verify the box binding and introduction's key hash.
- Ana publishes write 9 and crashes before posting positions. Ben reads 9
  by its next number. Neither a stale post nor a missing post hides it.
- After observing storage time T, list device folders, including unknown
  writers, then read each writer's entries through its next-number miss.
  Because those entries are never deleted, retained entries plus these reads
  contain every entry through T. Failures prevent finality; a time learned
  during this observation cannot be used retroactively as T.
- A recent completed catch-up certifies fully realized history and a
  confirmed positions post. Before 30 storage days have elapsed, writes
  published since it cannot be age-deleted, and its position protects any
  unconsumed write from the other deletion alternative. A pass with missing
  history cannot refresh this checkpoint merely by reporting `Synced`.
- Ben returns after 40 storage days with Ana's write position 8. Listing
  the audience's retained snapshots finds snapshot 3 even if 2 was deleted.
  Its checked prefix covers Ana 20, so he loads it and asks for 21. A newest
  snapshot covering no writer beyond his audience's current positions is
  not applied. Check multiple audiences and the common-point transaction.
- Away time is a fresh clock object's storage time minus the saved completed
  catch-up's storage time. Test the 30-day boundary, absent checkpoint,
  failed clock observation, reopen and suspension. Device wall-clock jumps
  cannot assert storage age. Clock and initialization work has its own count;
  idle passes do not write timestamps to keep this evidence fresh.
- Snapshot coverage is refreshed only for a needed reload, growth-triggered
  snapshot check or due retention candidate, never each idle pass. A changed
  positions post can make a retention candidate due. A coverage wait with
  unchanged prerequisites backs off, retaining signed prefixes and headers.
- Ana's invite captures copy 12. Carol reads 13 for Ben, then her own copy
  14; a restart resumes after retained copies. If the request disappears
  after an earlier miss, refresh copies and membership before reporting a
  decline. Polls share the persisted backoff and provider cooldown.
  After admission, earlier copies still have to be read for exposure;
  the invite's cursor is not a complete key-copy observation.
- Files are read by their fixed row paths. After Ben's access is cut off,
  one complete scan of his file folder finds orphans, including any upload
  that landed before cut-off. Cut-off excludes later publication by an
  in-flight session. Persist the scan and candidates; failure records no
  completion. Deletion still requires references and finality, and idle
  passes do not repeat the scan.

The storage and finality models take complete observations as inputs. They
do not prove adapter folder queries, retention-aware discovery, clock sampling,
key-copy numbering, or one-time orphan scans. Their inputs must be established
by these IO checks before applying their conditional results.

## Downloads and their visible outcomes

The model boundary includes eager cache fill and app file reads, sharing
range fetches and checked cache contents. Each unfinished file has one pending
subject with its first unmet condition; an immediate call retains its typed
cause as well. Check these outcomes through the real file reader:

- A network interruption or temporary storage failure records the storage
  reason and retries with the shared delay, retaining verified ranges.
- A file not yet stored records a wait for its exact path while the uploader
  can still supply it. A removed uploader or a reported missing source gives
  `FileUnavailable(FileMissingReason)` instead of endless automatic work.
- A missing audience key records `KeyUnavailable` for the write or snapshot
  carrying the file row, before a file reference is available. Acquiring
  the key retries retained encrypted inputs, without fetching them again.
- A content-hash mismatch records `Refused(ContentHash)`; failed chunk
  authentication records `Refused(Decryption)`. Neither publishes a successful
  file or pin or retries unchanged bytes automatically. An applicable reader
  update may recheck them.
- A successful range read authenticates bytes before exposing them. Completing
  a sequential read checks the whole-file content hash before its last buffer.

Ben's eager download has retained the first two ranges when Wi-Fi drops.
The pending list reports the network failure; progress still comes from those
two committed cache ranges. On retry he fetches the remaining ranges. A bad
content hash produces an integrity reason instead of a completed cache entry.

A Rust test must inject disk-full at cache publication and exercise the real
reader, database and disk boundary. It must preserve previously committed
cache facts, fail the requesting call with its disk cause, keep the unfinished
file visible as `Failed(LocalFailure::Disk)`, and publish neither a pin nor
positions that claim the failed work completed. This document adds no Rust test.

## Database state and subscriptions

Upload progress comes from provider-confirmed bytes in the durable resumable
session. One-request uploads go from waiting to absent from the queue only
after confirmed completion. Eager progress comes from committed cache coverage;
the device-reset notice comes from the replacement database. Each table-backed
app read and subscription executes the same pre-built query. Test restart and
failed commits through those queries, with no second in-memory progress source.

Carol restarts during a multipart upload. Her first `uploads()` result and
first `subscribe_uploads()` result name the same confirmed prefix. Neither
reports bytes merely handed to a socket. A failed database update makes the
query fail; it cannot report an empty pending list or completed work.
