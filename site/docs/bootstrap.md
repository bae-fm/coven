# Bootstrap

A device that joins or restores a store needs the current shared database without
replaying every retained Store commit. Coven publishes signed database snapshots
with exact commit coverage. The new device installs one snapshot, then pulls the
commits beyond that coverage.

## Snapshot contents

Snapshot capture reconstructs shared state at the exact accepted Store frontier
in a separate replay database. It excludes the local write journal, checks that
every accepted commit is represented, and exports the selected audience's rows.
Private rows and pending local changes do not become shared snapshot contents.
The live database is left unchanged.

The image retains the application schema and `user_version`. Signed metadata
binds the image hash, schema version, Store root, author, creation time, accepted
publication predecessor, and exact author-sequence/commit coverage. Continuing
membership, device, operation, and object-ownership evidence has its own place
in the image and metadata; it is not reconstructed by fetching the retired
publication prefix.

The database image contains catalog rows, not their blob files. Bootstrap
downloads every referenced `CacheEager` blob before accepting the store;
`CacheLazy` blobs remain fetch-on-read.

## Publication

Store commits and Store snapshots share one accepted publication order. A
snapshot names the exact current publication record whose accepted state its
image represents. Uploading the image or signing metadata does not accept it:
the publisher must conditionally advance the provider's current record from
that exact predecessor to the snapshot's publication entry.

Each candidate reserves its own metadata `ObjectSlot`. Its image and membership
rollup paths derive from that complete slot, including an opaque provider
identifier when present. Different candidates therefore own different image
and rollup objects even when their plaintext hashes match. The signed metadata
binds those objects, coverage, membership and device state, retained authority,
schema version, author, and creation time.

Before uploading, Coven durably records the exact image, rollup, metadata, blob
bindings, and publication attempt. It verifies the blobs and creates the image,
rollup, metadata, and immutable publication entry before attempting the
conditional current-record replacement. Local completion records accepted
coverage and transfers the image's continuing object ownership atomically.

A failed or lost response preserves the durable attempt. Retrying determines
its acceptance from the verified current publication before proceeding. If
another accepted commit wins the position, the snapshot is rebuilt over the
accepted state. If a newer accepted snapshot already fulfills the requested
coverage, the operation completes against that snapshot after retiring its
unused candidate objects. Conflicting bytes at an occupied exact slot are an
error.

Only an authorized Owner can publish a Store snapshot. An Owner's signature
alone does not establish acceptance; the verified current record and its
retained publication interval select the accepted snapshot.

## The membership rollup

A joining or restoring device needs verified membership authority before it can
open the Store keyring. Each Store snapshot therefore names a signed plaintext
membership rollup alongside its encrypted image. The rollup carries exact
membership heads, entries, and exact predecessor acceptance results for the
snapshot's membership state.

Those objects remain subject to signature, linkage, grant, and accepted-control
verification. The reader starts from its pinned Store root and required
membership floor. Missing or invalid required authority fails bootstrap;
a signed rollup does not independently establish which Store publications were
accepted.

## Selecting a snapshot

`PreparedSnapshotBootstrap::prepare` uses the verified current publication to
select the accepted Store snapshot. It verifies the required membership floor,
rejects an unsupported schema version before downloading the image, verifies
the image against its exact reference and plaintext hash, and binds the selected
authority to the staged database.

A pending Join can require the original snapshot accepted for its Attempt even
after newer snapshots have compacted that Attempt. Its retained Join proof
selects that exact snapshot and protects its artifacts. Bootstrap verifies that
retained authority instead of selecting another image with similar coverage.

Same-provider handoff uses `PreparedDeviceJoinSnapshot`: the transfer carries
verified installation authority and the accepted interval after its selected
snapshot. The receiver checks that closure and its accepted membership, then
downloads the selected image. It does not scan historical snapshot candidates.

## Installing coverage

The downloaded image and its signed coverage stay bound together through a
single-use `PreparedSnapshotBootstrap`. The value binds the destination path,
image hash, verified Store protocol root, snapshot hash, and exact coverage.
Its fields are private and it cannot be cloned.

Consuming it through `PreparedSnapshotBootstrap::install` rechecks the destination
and image bytes, opens the database with the application's normal migration
ladder and synced-table declarations, then installs the protocol root, snapshot
hash, and every exact covered position in one SQLite transaction. An invalid row
identity, migration failure, changed image, wrong store, or wrong destination
removes the incomplete database and returns the error.

The installed replay baseline binds its image to authenticated coverage and
publication authority. Pull verifies the accepted continuation above that base;
it does not fabricate a materialized commit row for each retired historical
entry.

Restore resolves Circle access for the receiving identity. The destination is
opened once and stays private until it can serve: the Store image installs
first, the receiving identity then re-resolves its own access against those
installed rows, and the selected Circle images and accepted packages install
beyond that base. Restore selects each Circle's verified image or founding base;
a same-epoch successor without a new image continues the recipient's exact
earlier bootstrap through verified control history. Missing or invalid required
images fail installation, and the failed attempt takes the database files and
every payload file it wrote with it. The snapshot author's cached access does
not grant the receiver access. Join and restore complete their required
membership, registration, eager-blob, and continuation work before returning the
Store.

## Schema versions

The snapshot image carries its SQLite schema and the signed metadata repeats its
schema version before download:

- A binary at or above the snapshot version opens the image and runs the same
  host migration ladder used by an existing device. The join or restore caller
  also passes its explicit policy for Coven's bookkeeping-schema ladder.
- A binary below the snapshot version refuses it with `SnapshotError::SchemaTooNew`
  before installing the database.

See [Schema evolution](/docs/schema-evolution) for live-commit version handling.

## Reclamation

An accepted Store snapshot closes the publication prefix it represents.
Covered Store packages can be reclaimed once their required rows, authority,
and blob ownership have transferred to the accepted snapshot or another live
owner. Store retirement does not wait for every device to acknowledge the
snapshot. Circle packages and images retain their separate Circle authority
and acknowledgement requirements.

The accepted successor carries exact pending deletion references for covered
publication entries and obsolete snapshot metadata, images, and rollups.
Pending Join artifacts remain protected until their operation releases them.
A later snapshot may omit a deletion obligation only after the provider
confirms the exact object is absent; a conflicting occupant is an error.
Physical artifact deletion uses that accepted inventory and does not append a
new publication entry for each object.

Reclamation preserves objects needed by retained replay, pending work, private
state, live blob bindings, and continuing control evidence. Package deletion
uses its verified authority and durable operation journal. Exact deletion
failures remain visible and resumable. The authoritative current-record slot
is preserved through compaction.
