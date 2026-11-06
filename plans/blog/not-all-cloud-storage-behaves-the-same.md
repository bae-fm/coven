# Not all cloud storage behaves the same

*Draft. Numbers are from coven's certification runs on 2026-10-13 and 2026-10-14, the last a full pass of
every reachable provider through the newest suites.*

Other titles to consider: "S3-compatible isn't S3-identical", "Same API, different guarantees".

## Why we care

Coven syncs an app's SQLite database between devices with no server of our own. Each person brings
storage they already have: an S3 bucket, Cloudflare R2, Google Cloud Storage, a self-hosted RustFS or
Ceph, or a consumer drive like Google Drive, Dropbox, OneDrive or iCloud. Devices write small encrypted
objects there and read each other's.

With no server in the middle, the storage is the only thing every device talks to. Whatever ordering or
mutual exclusion the system needs has to come from the storage's own behavior. So coven depends on a few
properties that most applications never notice:

- **Create-once.** "Write this object only if nothing is at that name" must succeed for exactly one of
  two writers racing to the same name, and fail for the other. Coven uses this the way a server would use
  a lock: it is how two copies of one device (say, a phone and a restored backup of it) find out they
  are both alive.
- **Partial uploads stay hidden.** A half-finished multipart upload must never be visible under its
  final name.
- **The timestamp means completion.** The time storage reports for an object must be when it became
  visible, not when the upload started. Coven reasons about time windows ("anything published more than
  a day after X is late"), and a start time can be minutes or hours early for a large upload.
- **Replacement has a revision.** Replacing an object must give it something (an ETag, a generation, a
  revision id) that changes, so a reader can tell it is reading the same version as before.
- **Listing lag is bounded.** A new object should appear in listings within some known time.

Most providers document some of this. Almost none document what happens when two requests race.

## How we test

Every provider goes through the same certification suites, run live against a real account or a local
container, with results recorded as evidence alongside the spec:

- **Suites 1-3:** each property above, one request at a time: a conditional write to a taken name, a
  multipart upload checked before and after completion, timestamps compared against the client clock,
  revisions on replacement, and listing lag measured over repeated trials.
- **Detection:** the same checks coven runs at setup, so we know what a person's bucket will be judged
  as when they connect it.
- **Suite 4:** concurrency. Two writers race a conditional write to the same name 200 times, and eight
  writers race 50 times. Every trial must have exactly one winner. This runs once with bucket versioning
  off and once with it on.
- **Suite 5:** the same races, but using exactly the publication method coven will use on that
  provider, including mixed races (a single PUT racing a multipart or copy publication of the same name).

The one-at-a-time checks pass on almost everything. The races are where providers differ, and a
provider that passes every sequential check can still fail them.

## What we found

### Racing writers

| Provider | Race | Result |
|---|---|---|
| SeaweedFS 4.48 | two conditional multipart completions | both accepted in 198 of 200 trials |
| SeaweedFS 4.48 | eight conditional multipart completions | more than one accepted in 49 of 50 |
| SeaweedFS 4.48 | conditional PUT vs conditional completion | both accepted in 2 of 200 |
| SeaweedFS 4.48 | conditional PUT vs conditional copy, unversioned bucket | both accepted in 14 of 200, then 17 of 200 on a rerun |
| SeaweedFS 4.48 | conditional copy vs copy; PUT vs PUT | one winner every trial |
| Ceph RGW 20.2.4, versioned bucket | two and eight conditional writers | 199 of 200 on the first run; on the full rerun every writer was accepted in every trial (2,400 acceptances where 900 were allowed) |
| Garage 2.4.1 | conditional multipart completions | more than one accepted in 197 of 200 (two writers) and 16 of 50 (eight) |
| Ceph RGW 20.2.0 | conditional multipart completion | overwrote a taken name (fixed in 20.2.1) |
| RustFS 1.0.1 | every race, versioned and unversioned | one winner every trial |
| Cloudflare R2 | every race | one winner every trial |
| Ceph RGW 20.2.4, unversioned bucket | every race | one winner every trial |

"Both accepted" means both clients were told their object was stored, and one of them was silently
overwritten. For coven that is lost data, and it is the exact case its stale-device detection exists for.

SeaweedFS is a good example of how narrow the failures can be. Single-request conditional writes are
correct. Multipart completions are not. We moved SeaweedFS's large uploads to a staging name plus one
conditional copy, and copy-against-copy then had one winner in every trial, in both bucket kinds. But a
small object published by PUT racing a large one published by copy, to the same name, still had both
accepted 14 times in 200 in an unversioned bucket. The checks for the two request types are apparently
not atomic with each other. In a versioned bucket every race passed.

Ceph is the opposite: unversioned buckets pass, versioned buckets fail. Coven refuses versioned Ceph
buckets.

### Conditional writes that don't exist

- **Garage** ignores `If-None-Match` on every write. It is a deliberate design choice (Garage does not
  coordinate between nodes for writes), so it can never offer create-once. Coven refuses it at setup.
- **Backblaze B2** answers `If-None-Match` on a PUT with 501 Not Implemented.
- **DigitalOcean Spaces** documents the header only for GET and HEAD.

### Timestamps

- **AWS S3** and **Garage** report a multipart object's start time, not its completion time. On AWS,
  coven publishes multipart objects by copy so the stored time is the copy's.
- **OneDrive** reports completion only via the copy's last-modified time, so coven publishes there by
  copy too.

### Listing lag

Median time for a new object to appear in a listing:

| Provider | Median |
|---|---|
| GCS (S3 interop) | 0.13 s |
| Cloudflare R2 | 0.21-0.22 s |
| Google Drive | 0.43 s one day, 0.99 s (max 5.9 s) the next |
| OneDrive | 2.7 s one day, 0.93 s the next |
| RustFS, SeaweedFS, Ceph, Garage (local) | 3-50 ms |

Time resolution differs too: S3 services and OneDrive report whole seconds, Google Drive and iCloud
milliseconds.

### What the docs don't say

For many providers the concurrent behavior is simply undocumented: Wasabi, Hetzner Object Storage,
Linode/Akamai, Storj's multipart path, Tigris's multipart path. OVHcloud's source shows a
check-then-write for conditional requests, which is the shape of the SeaweedFS bug. For these, the only
answer is to run the race. They stay uncertified until we do.

## What coven does with this

- Each provider has a profile that says how coven publishes on it (single PUT, conditional multipart, or
  staging plus conditional copy) and whether it is certified.
- Setup runs detection against the person's actual bucket and refuses one that fails a required check.
  It also refuses profiles known to fail races that the one-at-a-time detection can't see (SeaweedFS
  today, Garage, versioned Ceph buckets).
- The rules that depend on these properties are proved in Lean (for example: given an atomic conditional
  copy, staging plus copy has exactly one winner and never exposes a staging object). The proof says what
  coven needs, and the live suites check whether a provider gives it.

## One slot, three ways to publish it

*This section describes coven on 2026-11-06, partway through a change. What has landed and what
hasn't is marked at the end.*

Coven publishes each batch of writes into the next numbered slot, say `seq/7`, with a create the
storage accepts only if nothing is at that name yet. Most slots are a few kilobytes, but the format puts
no cap on a batch, so a slot can be large. The floor, the one object every device replaces, has the
same problem. On S3 services a slot could go up three ways.

**A. One request.** An object goes up in a single PUT carrying the create-once condition when it is at
most the lesser of the service's single-request limit and 32 MiB. The 32 MiB bound is coven's own: a
request carries its whole body, and without it a laptop would hold a 2 GB snapshot in memory to send it.
A crash here is harmless. Before sending, the device records in its journal the slot and the exact
bytes. After a crash or a lost reply it reads `seq/7` and compares: its own bytes mean the claim landed,
another device's bytes mean it lost the race, and nothing there means it sends the same bytes again.

**B. In parts, finished in place.** R2, RustFS, Ceph RGW and Versity Gateway did this, as could an
unknown S3 service whose detected settings chose it:

1. Start a multipart upload at `seq/7`. The start carries no condition and is never refused, so
   several devices can each have an upload open at that name at once.
2. Save the upload id the reply carries.
3. Send the parts.
4. Complete the upload with the create-once condition. The service refuses the completion if `seq/7`
   is already taken.

**C. In parts, by copy.** AWS and GCS did this for every object that goes in parts, and SeaweedFS for
every object, small ones included:

1. Upload to a name only this device uses, `<store>/.coven-uploads/<device>/<session>`, and finish the
   upload there. No condition is needed, since no other device writes there.
2. Copy that object to `seq/7` in one server-side request carrying the create-once condition
   (`If-None-Match: *`, or `x-goog-if-generation-match: 0` on GCS).
3. Delete the private object.

The object is dated by the copy's completion.

### Why the services differ

Path B is the services' own way to create a large object once, and coven used it wherever it held. It
didn't hold everywhere:

- **AWS** refuses a taken name at completion, but dates a multipart object from when its upload
  started. A copy is dated when it completes.
- **GCS** dates completion correctly, but its multipart completion doesn't enforce create-once, and
  its start refuses the generation condition.
- **SeaweedFS** refuses a taken name one request at a time, but two racing conditional completions were
  both accepted in 198 of 200 trials (the table above). Copies racing copies kept one winner. A PUT
  racing a copy did not in an unversioned bucket, so on SeaweedFS small objects go by copy too.

### The gap a crash found

Path B has a window. Between the start going out and the upload id being saved, the device can stop,
or the reply to the start can be lost. Then an upload exists at `seq/7` that nothing on the device
names.

At a name only the device uses, that is easy to clean up: every unfinished upload there is the
device's own, so its next attempt aborts all of them and starts over. Path C's private name is such a
name, and so are the device's own file and snapshot folders, where path B also put uploads.

At `seq/7` it can't. Other devices' uploads for the same slot may be open there. Coven tags each upload
with a random id in its metadata when it starts, but the S3 request that lists unfinished uploads
(`ListMultipartUploads`) gives each one's name, upload id and start time, not the metadata set at its
start. Nothing in the listing ties an upload to this device. Aborting all of them could end another
device's claim partway through, and picking one is a guess. So coven didn't guess: the claim failed with
`SessionIdentityUnknown`, a failure that never clears by itself. A device settles one claim before
starting another, so nothing else it wrote went out either.

Certification had passed those four services on path B. It checked what path B needs from the
service: completion refuses a taken name, an unfinished upload is invisible, and the object is dated
by completion. They do all three. The gap isn't the service misbehaving. It is what coven can find out
after its own crash, and no live race covers that. It turned up while modelling upload sessions in
Lean.

### One path for large uploads

The decision, on 2026-11-06: every S3 service publishes large uploads by copy, path C. A device's
unfinished upload then always sits at a name only it uses, and after a crash it aborts whatever is
there and starts over. S3 has no rename, so publishing from a private name is a copy, then a delete.
Each large publish costs one extra server-side copy. The bytes don't pass through the device again.
Objects up to the single-request size still go in one PUT, except on SeaweedFS.

That has two consequences:

- A certificate covers the publication it raced. The four services were certified completing in
  place, so each is certified again on its copy, under the same races: the copy must refuse a taken
  name atomically. R2 takes its own header for that, `cf-copy-destination-if-none-match`. A service
  whose copy fails is reported, with no quiet fallback to path B.
- One S3 copy request copies at most 5 GiB. Coven already refuses an upload past that, before sending
  anything, on AWS and SeaweedFS (`CopyTooLarge`). With copy everywhere the same limit reaches the
  other services.

Where this stands on 2026-11-06:

- **Landed:** paths A, B and C as above; recording an upload's plan before its start, so a crash at a
  device's own name is cleaned up; and `SessionIdentityUnknown` at a slot or the floor on path B.
- **In progress, on a coven branch, not landed:** the Lean model in which every upload's plan names a
  place of the device's own, and the spec's rewrite to one path. Still to come: the profile type that
  no longer offers completing in place, the code, the update to the test that compares the code with
  the model, and the four services' new certification runs. None of the four has been certified on its
  copy yet.

## Open questions for the post

- GCS hasn't run suite 5 yet (the test key lacked permission to read the bucket's versioning).
- Versity Gateway is next to test.
- Whether to name versions and dates for each result (they change: Ceph fixed its multipart bug in
  20.2.1).
- Whether to report these upstream first (SeaweedFS PUT-vs-copy, Ceph versioned conditional writes)
  and link the issues.
- Once copy publication lands, "What coven does with this" no longer lists conditional multipart as a
  way to publish on S3. Rewrite that bullet, and the slot section's status list, from what landed.
- The branch's spec draft says Ceph RGW 20.2.4's conditional copy refused no taken name, even one
  request at a time. Confirm against a recorded run before quoting it; if it holds, Ceph has no
  certified way to publish large objects.
