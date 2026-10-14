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

## Open questions for the post

- GCS hasn't run suite 5 yet (the test key lacked permission to read the bucket's versioning).
- Versity Gateway is next to test.
- Whether to name versions and dates for each result (they change: Ceph fixed its multipart bug in
  20.2.1).
- Whether to report these upstream first (SeaweedFS PUT-vs-copy, Ceph versioned conditional writes)
  and link the issues.
