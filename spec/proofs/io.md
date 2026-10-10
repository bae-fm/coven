# IO and observable work

This is the verification contract for [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), and the file calls in [E8](../api.md#e8-files-and-the-cache).
It is a specification of required checks, not an executable Lean proof.

## Requests and retained observations

Count provider requests, metadata and body bytes, lookup and redirect calls,
upload parts, retries, deletes and access requests separately. A provider
adapter must expose the actual calls behind a logical operation. Initialization,
eviction and interrupted transfers each have their own counts.

Ana has 12 devices and 20,000 old photos. With no changes or due work, her
pass reads no bodies, rewrites no positions and commits no database changes.
Complete discovery still charges every listing page and folder traversal;
§4.1 explains why next-number misses cannot replace discovery after deletion.
Increasing unchanged history must leave the body-read and local-work counts
unchanged, without disguising increased listing traffic.

Checked headers, prefixes, decoded entries, peer revisions and file-reference
facts survive passes and reopen. A missing key cannot cause the same bytes
to be downloaded again. A failed transfer can retry its missing suffix;
all repeated bytes are charged. A refusal cannot be cleared by evicting its
bytes. A provider cooldown applies to every worker in its scope and survives
restart through the stored deadline and wait duration.

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
