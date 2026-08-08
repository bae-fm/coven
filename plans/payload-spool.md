# Payload spool: bytes leave rows

## Problem (measured 2026-08-07)

`coven-replication` burns 1,180s of the suite's 1,308s CPU, almost entirely in
serde_json. One 18s circle test round-trips 346MB of JSON (84 serializations,
98 deserializations of a payload that grows to 4MB). Cause: large payloads live
inside SQLite rows — as BLOBs or as `Vec<u8>` serialized into JSON number
arrays (~4× inflation) — and the circle journal rewrites its whole payload on
every progress step, with a full throwaway parse (existence preflight) before
each rewrite. In production the same shapes cap the store at SQLite's 1GB row
limit and buffer whole database images in memory.

Inventory of offenders:

| Table / column | Payload | Lifetime |
|---|---|---|
| `circle_operations.payload` | journal JSON embedding all prepared object bytes | per operation, rewritten per step |
| `outbound_store_snapshot`, `outbound_circle_snapshot` | `image_bytes` BLOB (plaintext) + `image_prepared` JSON (ciphertext) | staging |
| `retained_replay_baselines.image_bytes`, `.authority_bytes` | full replay-baseline DB image | **durable** |
| `circle_bootstrap_coverage.image_bytes` | retained bootstrap image per circle | **durable** |
| `remote_objects.state`, `protocol_inert_objects.state` | `RemoteObjectBytes`: canonical plaintext + inline ciphertext, JSON | object lifecycle |
| `store_writes.changeset`, `.inverse_changeset`; `store_write_partitions.changeset` | session changesets, 3× copies | until published |
| `outbound_membership_mutation.plan_bytes/progress_bytes` | journal embedding `PreparedExactObject`s | per mutation |

Legitimate small BLOBs (acks, registrations, protocol roots, control bytes,
`circle_current_state`) stay as they are.

## Design

**Durable bytes are files in the store directory; rows hold hashes, references,
and facts.** One file-store discipline — the one the snapshot-blob spool
already uses (`PreparedSnapshotBlob.spool_path`, `snapshot_blob_spool_cleanup`,
`drain_spool_cleanup`): write file, fsync, then commit the row that references
it; delete via cleanup-intent rows drained by the owning flow.

Two lifecycle owners share the file layer and nothing above it:

- **Host blobs** (existing): leases from row references, audience packaging,
  local/remote transitions, eviction. Unchanged.
- **Internal payloads** (new): owned by their operation/record row. Never
  leased, never packaged as blobs, never evicted. Deletion rides the flow that
  deletes the owning row.

Crash orphans: spool files are content-keyed (`ObjectHash` of the stored
bytes), so a retry of a failed insert rewrites the same path idempotently. A
file whose owning insert never committed is inert garbage bounded by that one
operation's content; no background sweeper exists or is added — deletion only
ever rides row deletion.

Schema policy: **clean break.** No migration of existing rc stores.

## Stages

Each stage is a single-concern branch merged ff-only to `main` per the fast
background-feature-loop. Every stage runs the touched crates' tests locally
before merge.

### Stage 1 — `Signed<T>` caches its digest

`coven-protocol/src/store_commit/signed.rs`. `digest()` currently re-runs
`domain_json` (full serde_json serialization of the body) on every
`hash()`/`verify_by()`. Add a cached digest:

- Cache field: `std::sync::OnceLock<ObjectHash>`, `#[serde(skip)]`.
- `PartialEq`/`Eq` become manual over `(version, body, signature)` — the cache
  must not participate.
- Invalidation: only `body_mut()` — it replaces the cell with a fresh
  `OnceLock`. `resign()` does not invalidate (the digest covers version+body,
  not the signature). `sign`/`sign_by_device`/deserialize start empty.
- Tests: repeated `hash()` returns one value; `body_mut` tamper still fails
  `verify_by` (existing tampering tests already cover this — they must stay
  green); serde round-trip equality.

### Stage 2 — payload spool primitive — LANDED (2edcea0c)

Module `coven-database::payload_spool` (child of `store`, where the private
connection lives); path scheme on `StoreDir`
(`payload_spool_dir`/`payload_spool_path`), since `StoreDir` centralizes
on-disk layout.

As landed, two deliberate changes from the original sketch:

- Cleanup obligations are keyed by **hash**, not path
  (`payload_spool_cleanup(payload_hash TEXT PRIMARY KEY)`): the store dir is
  ambient, so a recorded absolute path breaks silently if the directory moves;
  the hash is the file's identity. Table participates in `REPLAY_TABLES` as
  `Preserve`.
- `write(&[u8]) -> ObjectHash` **computes** the hash (via `AtomicStagedFile`:
  temp sibling → fsync → rename → parent fsync), so a file whose name
  disagrees with its content is unrepresentable. Bytes-only API — the
  streaming arm arrives with its first caller (stage 5 changesets).

`read` fails loud (`Missing{hash,path}`), no verify-on-read; `delete` treats
absence as success (drain retry safety) and fsyncs the parent. Six tests incl.
a concurrent-reader atomicity test (sabotage-verified against a direct write).

Known gap, owned by stage 3: `StoreDir::remove_orphaned_blob_temps` does not
yet sweep `spool/payloads` temp files; extend it in the same change that
introduces the first production writer.

### Stage 3 — circle journal split by mutability — LANDED (967967fa + f74d9301)

Receipts: `re_adding_a_removed_member_...` 25s → 10.0s; full workspace
1607/1607. Landed with the plan's rulings plus: `candidate_owned_objects()`
dispatch (shared Circle objects have no `remote_objects` record — the old code
silently filtered them); the size pin asserts "prepared column < the bootstrap
image it names" (measured ~200KB for a 4.76MB operation — `creation`'s flat
leaf ciphertext and `commit_bytes` remain, stage 4/5 territory); the
publication stack guard moved 1.0→1.5 MiB (real ~3% poll-frame regression,
bisected; boundary boxing doesn't reduce it — cap still catches a dropped
opt-level); the audit caught drain_cleanup with zero callers — now called by
each obligation-committing flow post-transaction and on operation resume
(f74d9301), each drain sabotage-verified.

The core amplification fix.

Schema:

```
circle_operations(operation_id, circle_id,
    prepared,   -- KB-scale JSON: intent, policy, commit_ref, commit_bytes,
                -- prepared object *refs* + step names; written at INSERT, never UPDATEd
    phase)      -- small JSON: ready / waiting_for_close_responses / finalizing
                -- / blocked{block, phase} / discarding; updated on transitions only
circle_operation_uploads(operation_id, step,
    PRIMARY KEY (operation_id, step),
    FOREIGN KEY (operation_id) REFERENCES circle_operations ON DELETE CASCADE)
```

Types (`coven-protocol/src/circle_journal.rs`):

- `PreparedCircleOperation.prepared_objects` becomes
  `BTreeMap<String, ExactObjectRef>`; the bytes go to the spool at preparation
  (one file per object, keyed by the object's stored-bytes hash).
- `CircleOperationProgress` keeps its variants as the domain phase enum, but
  the persisted form splits: `prepared` column holds the operation,
  `phase` column holds the variant + `Blocked` payload.
- `uploaded: BTreeSet<String>` leaves the struct; loading a journal joins the
  uploads table.

Database (`coven-database/src/store/circle_controls.rs`,
`circle_operation_records.rs`):

- `complete_circle_operation_upload_step` → single
  `INSERT OR IGNORE INTO circle_operation_uploads`; FK failure is the
  fail-loud "operation disappeared" path. The two full-journal clones and the
  preflight `load_circle_operation_on` existence check are deleted.
- Phase transitions (`begin_circle_operation_finalization`, blocking,
  discarding) → `UPDATE ... SET phase = ?`.
- `insert_circle_operation` writes spool files for prepared objects before the
  transaction; the transaction inserts `remote_objects` records (stage 4
  converts their inline bytes) and the operation row; discard/complete flows
  enqueue spool cleanup intents for the operation's objects in the deleting
  transaction and drain after commit.

Publication (`coven-replication/.../circles/publication.rs`):

- `create_or_read_step` reads the step's bytes from the spool by ref (and
  verifies against `ExactObjectRef` before upload — same check
  `PreparedExactObject::new` does today).

Lifecycle rulings (settled after lookover on the branch; these bind):

1. **`prepared` is replaced exactly once, at the close→finalization phase
   boundary** — `begin_circle_operation_finalization` installs the freshly
   prepared operation (new commit graph), and the same transaction deletes
   that operation's `circle_operation_uploads` rows and enqueues
   `payload_spool_cleanup` for the superseded payload's objects. Step names
   collide across phases by design (per object kind), so stale upload rows
   must never survive the boundary. The invariant is "never UPDATEd per
   upload step," not "never UPDATEd."
2. **`PreparedMergeCandidate` drops its never-read `prepared` byte field**
   (no production consumer reads `.stored_bytes()` off it or its
   `BlockedMergeCandidate`); the Circle discard path carries refs only.
   Scoped to `PreparedMergeCandidate` — `ExactProtocolObject`'s own
   `prepared` field gets judged in stage 4 when `remote_objects` converts.
3. **Until stage 4, insert paths take the byte-carrying prepared map as an
   explicit parameter** (`insert_circle_operation`,
   `insert_circle_operation_superseding`,
   `begin_circle_operation_finalization`), checked against the journal's
   refs, because `remote_objects.state` still stores bytes inline. No
   `StoreDir` threading into coven-database; the parameter disappears in
   stage 4.
4. **Per-step remote-object marking survives; the per-step rebuild dies.**
   The uploaded state on `remote_objects` is load-bearing (discard cleanup
   decides absence-verified deletion from it). The step transaction is
   `INSERT OR IGNORE` into uploads plus a targeted single-object load →
   `mark_uploaded_verified()` → update, object id derived from the step's
   ref in the parsed `prepared` column. The whole-graph recompute
   (`closed_remote_objects` per step) is deleted.

Correctness: byte-identical resume still holds — bytes are durable in the
spool from preparation; the journal's identity checks (`validate_identity`,
operation/circle id binding) move onto the `prepared` column parse. The
uploaded-set semantics are identical (set-insert idempotence →
`INSERT OR IGNORE`).

Tests: the existing circle publication/rotation suites are the coverage — they
must pass unchanged in behavior. Add: journal payload size pin (an operation
whose objects total N MB stores a `prepared` column under 64KB); resume mid
upload (crash between step insert and next step) — existing
interrupted-*-resumes tests cover this, keep green.

### Stage 4 — `remote_objects` and retained images onto the spool

Rulings (settled after lookover; these bind):

- **A (corrected 2026-08-08; the first version's premise was inverted).**
  What survives snapshot projection is stored-blob rows AND retained-replay
  **package** rows (`pin_retained_merge_objects_on` pins packages + bound
  blobs; membership rows are never pinned and never travel). But traveling
  package rows are needed by identity only: the redundant byte-compare in
  `validate_retained_package_remote` narrows to its semantic-hash check, the
  reclaim parses narrow to identity comparisons (`changeset_size` is in the
  reference), and replay's actual package bytes travel in
  `retained_merge_materializations.canonical_input`. Therefore **carry-set =
  `SharedLiveSetObjectDomain::StoredBlob` only**; every other domain spools.
  Both narrowings are in scope. (Typed `BlobLocator`: future cleanup, out of
  scope.)
- **Payload representation**: `RemoteObjectBytes` deleted; record field
  becomes `payloads: RemoteObjectPayloads` with flat variants naming where
  bytes live — `SpooledInline` (plaintext under `identity.semantic_hash`,
  ciphertext under `identity.object.stored_hash()`), `SpooledExternal`,
  `RowInline`, `RowBlob { locator_bytes }` — and `validate()` checks
  variant↔domain agreement, making the transit guarantee structural.
- **Discovered pre-existing defect, NOT this stage**: nothing pins
  membership entry/head/resolution rows into snapshots, so a restored device
  replaying a retained materialization that references membership objects
  hits "prepared remote object … is absent" today regardless of bytes.
  Tracked separately.
- **Execution spec**: plans/payload-spool-stage4-spec.md (scout-derived;
  blast radius, release-site inventory, owners-table transaction shape,
  six-step order, sabotage points). Three variants only —
  `SpooledInline | SpooledExternal | RowBlob` (`RowInline` would pre-shape
  for the membership gap, task #29's problem).
- **`circle_bootstrap_coverage` is DEFERRED out of stage 4**: it sits in
  `SNAPSHOT_PRESERVED_NON_SYNCED_TABLES`, so its `image_bytes` travel inside
  published Store snapshots — the transit position the carry-set ruling
  exists for. Its bytes stay in-row this stage. Open question for a later
  stage, to settle with evidence: what does a restored device actually read
  from the traveled bytes — and since the bootstrap image also exists in
  cloud storage as a protocol object, can the traveling row carry the ref
  while bytes are fetched on demand? (Also note: as-is, every published
  Store snapshot carries every circle's bootstrap image inline — an
  image-inside-image multiplier worth killing once the reader question is
  answered.) `retained_replay_baselines` is NOT snapshot-preserved and
  converts as planned.
- **`DeviceHead` owns-its-head-commit check**: the commit reference is
  extracted from semantic bytes ONCE at construction and carried as
  structured record state; the narrowed `validate()` checks the carried
  value byte-free, and `validate_payload()` confirms carried-vs-bytes
  agreement wherever bytes enter. Byte-derived facts become data at the
  boundary — no parsing inside `validate()`.
- **B. Payload ownership gets a refcount table** —
  `payload_spool_owners(payload_hash, owner_key, PK both)`. Registering
  owners happens in the transaction creating the referencing row; the
  transaction dropping the LAST owner enqueues the cleanup obligation
  (delete owner → if none remain, insert obligation, one transaction).
  This kills the live collision (circle activation releases every prepared
  object's hash while surviving `remote_objects` rows still need the file)
  correctly by construction rather than by an unenforced "no two rows share
  a hash" assumption. Stage 3's release site converts onto it.
- **C. `RemoteObjectRecord::validate()` narrows to identity + ownership
  state.** Byte agreement is enforced where bytes enter: construction and
  spool reads (ref-verify on read, as circles' `create_or_read_step` does).
  Hash-named files make bytes-vs-hash structural; per-load re-parse and
  re-signature-verification of semantic bytes was the remaining CPU sink —
  untrusted data is verified at ingestion (pull/parse paths), not on every
  local record load.
- **`RemoteObjectBytes` is deleted, not converted**: both payloads are
  already named by the record's identity (`ExactObjectRef` = slot + size +
  hash; `semantic_hash`), so the struct degenerates to a representation tag.
  No parallel hash+length fields duplicating the identity.
- **Writer ruling (settled at step 3, reversing the "byte-map dies" call):
  the persisting transaction installs the payloads.** `ClosedRemoteObject
  { record, payloads }` keyed by exactly `record.payload_claims()` — a
  claim whose bytes are missing is unrepresentable; `persist_*_on` writes
  the spool files (blocking IO on the connection thread — the sanctioned
  pattern since the drain moved there), registers claims, and inserts the
  row in one place. One writer, one registrar: "row names a file that
  exists" is a fact of one function, not a convention across ten producing
  flows. Consequently the stage-3 byte-map parameter SURVIVES with a new
  justification — it feeds spool installation at persist. (The alternative
  — every producing flow spools for itself — was rejected: pulled records
  need spooling too (`cache.rs:465-480` reads both payloads of pulled
  membership records), so the convention would span every ingest path,
  unenforced.) `&StoreDir` reaches the persisting/reading closures as a
  parameter; production read sites inside closures
  (`pending_publication.rs:151`, `publication.rs:335`,
  `materialization_io.rs:150-190`, `cache.rs:453`) use blocking spool
  reads the same way.
- **`ProtocolInertObject` carries no payload** (settled during step 3):
  with `validate()` narrowed and the head commit carried in the
  `DeviceHead` domain, nothing reads the inert plaintext —
  `is_terminal_head_for` uses the carried commit. The field is deleted;
  the remote→inert transition is a plain release, not an ownership
  transfer. No bytes kept for hypothetical future readers.
- **Context struct instead of `&StoreDir` fan-out** (settled during step
  3): the retained-replay and candidate-record free-function families take
  a `{ conn, store_dir }` context in place of bare `&Connection`
  (`SqlReadContext` is the repo precedent), so functions with no interest
  in files stop carrying a store directory. Required anyway by
  `owner-construction-check`, which forbids owner methods accepting
  `StoreDir` at runtime — context param or field, never a method argument.
- Before any step-3 commit: the three `unwrap_or_default()` placeholders
  on `Option<&[u8]>` locator reads (`blob_records.rs` ×2,
  `blob_bindings.rs` ×1) become fail-loud — they are masking as written.
- **Ruling C refinement (after step 3's receipts moved UP, 23.6→29.0s):
  local spool reads do not re-verify.** The trust model, stated once:
  verification happens at **ingestion** (pull/parse, construction) and at
  **egress** (before upload — the `create_or_read_step` re-check), never on
  routine reads of local durable state. Spool files written by verified
  construction carry the same trust as the database's own rows — which
  ruling C already stopped re-verifying per load; re-verifying their spool
  files per read was the same tax reintroduced through the side door.
  `spooled_semantic_payload`'s per-read `validate_payload` goes; replay
  reads return bytes. No per-read re-hash either — that is the cost being
  removed, and SQLite pages get no such check.
- **Step 3 LANDED** (56703280..1c2b280e): 1610/1610, gates green. Ten
  raw-SQL `remote_objects` deletes found stranding claims — all rerouted
  through `delete_remote_object_on` (the two `snapshot_image.rs` deletes
  deliberately exempt, now commented). `StoreRecords { conn, store_dir }`
  context + `StoreRecordTransaction` landed;
  `ClosedRemoteObject::payload_bytes()`; sabotage receipts re-pointed at
  row existence after the first version flipped both sides together.
- **`ExactProtocolObject`**: `object` field collapsed (was provably
  `prepared.reference()` everywhere; six comparisons were tautologies).
  `bytes` KEPT — sole blocker is `verify_readback`'s plaintext comparison,
  which stage 6 deletes: **re-judge `bytes` at stage 6.** Also flagged for
  stage 5: `snapshot_records.rs:137` clones one whole image into both
  `value` and `bytes`.
- `open_database_image` file-open moves with the deferred
  `circle_bootstrap_coverage` conversion (its input is pull-decrypted
  memory, not a spooled row).
- Step-1/2 landed deviations (accepted): whole-set claims per owner
  (`set_payload_owner_claims_on` — release-then-register and
  register-then-release both have windows at the finalization boundary;
  set replacement has none); a payload entering a claim set discharges any
  deletion it was owed (re-prepared content shares the hash of a deleted
  payload); the cleanup drain runs on the connection thread, serialized
  against claim transactions.
- `ExactProtocolObject.prepared` re-judged at stage end under ruling A
  (it remains the ciphertext carrier to spool writers; delete if that role
  evaporates).
- **Ruling C refinement LANDED** (51bdf3ca). Three local readers dropped
  their per-read `validate_payload`
  (`materialization_io::spooled_semantic_payload`, `cache.rs`'s retained
  membership bytes, `PreparedAudiencePackage::from_remote`); the remaining
  callers are constructors, `CandidateObjectGraph::close`, and tests. Full
  `coven-replication` 165.7s → 144.7s; receipts 32.1 → 29.2s and
  13.3 → 11.8s (measured on this machine, whose HEAD baseline reads ~10%
  above the 29.0/11.0 recorded earlier). Still above the pre-step-3 marks,
  so profiled rather than guessed again — see below.
- **Where the remaining time is (profiled, `sample`, one nextest process,
  13,032 non-idle samples of 132,089).** Half the busy CPU is the `dev`
  profile itself: 29.6% `core::ptr`/`core::slice` `precondition_check` +
  `is_aligned_to`, 20.7% unoptimized slice/vec/iter shims. Real work:
  10.7% `F_FULLFSYNC`, 9.5% serde_json serialize, 7.9% ed25519, 6.1%
  hex/`ObjectHash`, 5.3% sha256, 1.4% sqlite. **No hotspot is left** — the
  protocol side is `Signed::parse_at`/`to_bytes`/`digest`/`verify_by` and
  `domain_json` spread over dozens of loaders, none above ~2%, i.e. "every
  load re-parses and re-verifies a signed object out of its row". The
  per-read spool verify was one instance of that family; the family is
  stage 7's re-measure target, not a single fix.
- **The spool fsyncs harder than the database it commits with** (new, from
  the same profile). Under `fcntl`: `AtomicFile::replace` → `write_atomic`
  → `File::sync_all` 496 samples, `write_atomic` →
  `flush_directory_blocking` 491, tokio `AtomicStagedFile::sync_all` 308,
  spool-delete parent sync 90 — 1,385 total. SQLite's own commit fsync in
  the same run: **14**. Rust's `File::sync_all` is `fcntl(F_FULLFSYNC)` on
  macOS; SQLite at `synchronous = FULL` without `PRAGMA fullfsync` uses
  plain `fsync`. So each payload write pays two F_FULLFSYNCs while the row
  naming it commits at a weaker tier, and the pair is only as durable as
  the row — the extra strength buys nothing and contradicts the stance
  under "After the arc". Fixing it edits
  `coven_foundation::atomic_file`, shared with the blob spool, the key
  envelope, and config: a durability policy decision, so it belongs with
  the durability-contract commit rather than inside a stage.
- **Step 4 (retained replay baselines) LANDED** (11322c80).
  `retained_replay_baselines` already had `image_hash`, so the image needed
  no new column — the existing hash is the spool name; only
  `authority_bytes` gained an `authority_hash` partner. Owner key
  `retained-replay-baseline` claims both, registered in the insert
  transaction and replaced whole when the authority is replaced; both claim
  sites sabotage-verified. **`RetainedReplayBaseline::open_image` keeps
  deserializing into memory rather than opening the file in place** — a
  replay writes into what it opens (installs Circle bootstrap tables,
  applies materializations), so it needs a private copy; the buffer is a
  streaming question for stage 5. Deleting the image payload now fails the
  load loudly instead of yielding an image from elsewhere.
- **Stage 4 receipts, one machine state** (this box, full-crate parallel
  `nextest -p coven-replication`; absolute numbers on this box run ~10% above
  the 29.0/11.0 recorded earlier, and drifted again mid-session when the disk
  filled and the target dir was rebuilt — so all three rows below were
  re-measured back-to-back on the final state):

  | commit | `routing_conflicts_…` | `effective_access_failure::…` | suite | tests |
  |---|---|---|---|---|
  | 1c2b280e (stage-4 step 3, start) | 35.09s | 17.04s | 197.7s | 632 |
  | 11322c80 (ruling C + baselines) | 33.03s | 15.41s | 177.0s | 634 |
  | 41dc6f84 (final) | 33.17s | 15.63s | 184.6s | 635 |

  Receipts −5.5% and −8.3% across the three commits; step 5 is time-neutral
  (its journal is per-mutation, not on the pull path). Suite total is the noisy
  figure — 9% run-to-run on identical binaries — so read the receipts, not it.
  The receipts remain above the pre-step-3 marks; the profile above says why.
- **A store is a directory, not a file.** Two tests `VACUUM INTO`-copied a
  store's database into a fresh directory and opened the copy, which now
  produces rows naming payloads that are not there
  (`pull_tests::retained_input_collision_rolls_back_remote_rows_and_materialization`
  and the pre-close Circle base in `circles/tests/publication.rs`). Both
  copy the payload spool alongside, via
  `test_helpers::copy_payload_spool`. The third `VACUUM INTO` site
  (`store/acknowledgements/tests.rs`) copies within one directory and
  shares the spool. Expect the same break from anything treating the
  `.sqlite` as the whole store.

- `RemoteObjectBytes`: `canonical_semantic_bytes` and
  `RemoteStoredRepresentation::Inline.bytes` become spool refs (hash + length);
  record JSON keeps refs only. Readers (`load_remote_object_on`, upload paths,
  reclaim verification) read via spool.
- `retained_replay_baselines.image_bytes`/`authority_bytes` and
  `circle_bootstrap_coverage.image_bytes` → hash columns + spool files.
- `pull_replay::open_database_image` opens the spooled file path directly
  (rusqlite open on file) instead of deserializing a memory buffer.
- `outbound_membership_mutation` plan/progress journals convert their embedded
  `PreparedExactObject`s to refs the same way.

**Step 5 LANDED as a deletion, not a spool** (41dc6f84; ruled after the
analysis below). No `membership-mutation` owner key, nothing spooled. Five of
the six fields collapsed to *nothing* rather than to `ExactObjectRef`: a
sibling `*Ref` already carried the same `ExactObjectRef`, so once the bytes
went the field was a duplicate of a duplicate, and the comparisons over it
became `x != x` — the same collapse `ExactProtocolObject.object` hit earlier
this stage. Only `PreparedStoreOperationCommit.prepared_head` had no sibling
ref; it became `head_object: ExactObjectRef`. One primitive replaces every
stored copy — `membership_mutation::prepare_exact_object(object, value)`
serializes and hands it to `PreparedExactObject::new`, which verifies against
the reference; validators call it through `binds_exact_object`, upload sites
call it directly. Sabotage receipt: stubbing `binds_exact_object` to `true`
lets both substituted-slot cases through
(`prepared_membership_transition_rejects_substituted_slots_and_bytes`).
Journal receipt: `the_membership_mutation_journal_carries_no_object_it_already_names`
pins that the staged plan holds no `entry_object`/`head_object`/
`resolution_object`/`prepared_head` and exactly one `stored_bytes` — the
sealed keyring. Validators got no new work: they already serialized each value
to compare it against the stored copy.

The analysis that produced the ruling: every one of the five fields named for
conversion
holds bytes that are a byte-for-byte re-encoding of a typed field sitting
beside it in the same struct, and a validator refuses any other value:

| field | sibling it duplicates | validator |
|---|---|---|
| `PreparedMembershipPublication.head_object` | `.head` | membership_mutation.rs:49 |
| `PreparedMembershipTransition.entry_object` | `.entry` | membership_mutation.rs:109 |
| `PreparedMembershipPublication.entry_object` | `.entry` | membership_mutation.rs:30 (runs the above) |
| `PreparedStoreOperationCommit.prepared` | `.commit` | prepared_commit.rs:297 |
| `PreparedStoreOperationCommit.prepared_head` | `.head` | prepared_commit.rs:304 |

Same for `ResolveMutationPlan.resolution_object` vs `.resolution`
(membership_mutation_journal.rs:347). `to_bytes()` on all of these is
`serde_json::to_vec(self)` (signed.rs:181; the three types are `Signed<T>` —
batch_commit.rs:32, heads.rs:18, membership.rs:355). The tell at the consumer:
`prepared_commit.rs:70-75` passes `&self.commit.to_bytes()` and
`self.prepared.stored_bytes()` as two arguments the validator forces equal.

So spooling them writes a third copy of each value — the row already carries
it as structure — and buys an owner key, a claim-set rewrite per progress
step, drains, and sabotage receipts for bytes that need not exist anywhere.
The alternative, and the one the campaign's own precedent points at (stage 3
ruling 2 dropped `PreparedMergeCandidate.prepared`; stage 4 deleted
`RemoteObjectBytes` rather than converting it): those five fields become
`ExactObjectRef`, and the upload sites rebuild
`PreparedExactObject::new(reference, serde_json::to_vec(&sibling)?)`, which
re-verifies the reference against the bytes so a mismatch fails loud exactly
where the object leaves the device. Upload sites: membership_commands.rs:190,
:203, :655; membership_publication.rs:70, :87, :284, :327. The row loses the
JSON byte-array copy (~4x inflation of the same content) and keeps the
structure. The one genuinely-carried payload in this journal is
`PreparedWrappedStoreKey.object` (wrapped_store_key.rs:265) — the sealed
keyring has no sibling value — and it is not in the conversion list.

**RULED: the deletion, not the spool.** The five fields (plus
`ResolveMutationPlan.resolution_object`) become `ExactObjectRef`; upload
sites rebuild `PreparedExactObject::new(reference, to_bytes(&sibling))`,
failing loud at egress on any mismatch. Nothing is spooled; no
`membership-mutation` owner key exists. `PreparedWrappedStoreKey.object`
stays carried (no sibling). Derivable bytes are never stored — this
supersedes the spec's step 5.

### Stage 5 — snapshot staging and changesets stream through files

- `outbound_store_snapshot`/`outbound_circle_snapshot`: drop `image_bytes`
  (plaintext) — the binding check uses the plaintext hash already present in
  `image_ref`; `image_prepared` ciphertext moves to the spool. The vacuumed
  image file is kept (moved into the spool) instead of `read_and_discard`
  into memory; projection runs against the file in place (it already does —
  `project` operates on the temp DB); encryption reads the file.
- Changesets: `store_writes.changeset` → spool file via the session streaming
  APIs (`sqlite3session_changeset_strm` family; `invert_strm` is already
  used). `inverse_changeset` column dropped — derived by `invert_strm` on
  demand at rollback. `store_write_partitions.changeset` dropped — partitions
  derived from the changeset at packaging time (the derivation code exists;
  it currently persists its output).
- Whole-image encryption may remain single-buffer in this stage if a chunked
  image format is a protocol change; if so, note it in the stage commit and
  leave a `Sentinel:`-style comment is NOT used — instead the stage report
  names it as the remaining buffered site.

**LANDED** (ac369fc8). The vacuumed plaintext image remains a file and both
its plaintext and ciphertext are content-addressed spool payloads claimed by
the snapshot journal. Whole-image encryption remains the one buffered image
operation because changing it requires a chunked protocol format.

The raw Store write changeset is streamed into the spool and now includes the
private audience and row-route tables needed for exact rollback. Its inverse
is generated at discard time. Exact audience partitions are also spooled:
partitioning synthesizes historical routing and ancestor materializations
that are absent from the captured changeset, and later live database state
cannot reproduce those exact bytes. This supersedes the proposed derivation
of partitions at packaging time.

A failed operation does not delete an installed, unclaimed hash: another
writer with identical bytes may be between installing that same file and
committing its claim. Such a file is inert; failed temporary siblings are
discarded before installation. The single stage self-review covered this
race, cleanup on failed Store calls, Store-write claim release in test
deletion, staged audience rollback, and owner-boundary access. Verification:
813/813 `coven-database` and `coven-replication` tests, the owner-boundary
check, formatting, and clippy with warnings denied.

### Stage 6 — readback removal

- Delete `verify_readback` as a post-upload step (store snapshot publication,
  circle step publication). Upload integrity = provider checksum verification
  at upload time (S3 family already verifies content checksums server-side;
  ensure the SDK sends them).
- Google Drive's non-atomic create: the adapter resolves ambiguous creates via
  listing + `md5Checksum` metadata — no body downloads. The
  `ambiguous_exact_create` test family moves to metadata-based resolution.
- `create_or_read_step` stops re-downloading: create-if-absent + metadata
  comparison; a body GET remains only where a same-key collision must be
  content-inspected (cloned-identity commit slots), and the stage documents
  each surviving GET.

**LANDED.** Exact creates now carry an `ExactUpload` whose bytes or spool file
can be reopened for provider-native verification. The host selects
`upload_checksum`, `metadata_hash`, `readback`, or `unchecked` locally; join and
restore data never carry that policy. S3 sends SHA-256 on bounded and multipart
uploads and probes both a matching upload and deliberate checksum rejection.
Drive compares `md5Checksum`, Dropbox compares its block-based `content_hash`,
OneDrive compares `sha1Hash`, and CloudKit compares the hash in the manifest
committed atomically with its parts. Unchecked accepts only a witnessed create
response: it does not turn an occupied slot or lost response into an unverified
success.

Protocol journals open and compare their retained prepared bytes before
publication. Circle publication reopens its payload spool even for an uploaded
step, but no longer downloads that step; protocol and blob upload receipts pin
zero provider body reads. The only application post-create body GET is the
occupied Merge-head path, where a `SlotCollision` must be opened and verified
to distinguish an alternate activating head from a competing commit. Provider
probes retain their explicit full/range/cross-principal reads, and selecting the
`readback` policy deliberately adds the configured full-body verification.

### Stage 7 — enforcement + re-measure

- Schema gate test in `coven-database`: the set of BLOB columns and
  JSON-bytes-carrying columns equals an explicit allowlist (the KB-class
  artifacts); any new large-payload column fails the test with a pointer to
  this design.
- Full workspace suite; nextest timing run + `sample` profile of the previously
  slowest tests; record the numbers here. Expectation: replication CPU
  collapses from ~1,180s; the 31s pull test and 25s circle tests drop to
  seconds.

## After the arc — pin the durability contract

Independent of the spool stages; lands as its own commit once stage 7 closes.

Coven rides SQLite's compiled default (`synchronous = FULL`) without choosing
it; the journal bridge (atomic local intent → idempotent remote steps) depends
on commit-means-on-disk, so the default becoming `NORMAL` would silently
manufacture the cloned-identity scenario out of a power cut.

- `open_connection` (coven-database/src/connection_io.rs) sets
  `PRAGMA synchronous = FULL` explicitly, commented as the journal-bridge
  anchor.
- Test: `PRAGMA synchronous` reads back 2, `journal_mode` reads back `wal`.
- Deliberately not taken: `PRAGMA fullfsync` (macOS `F_FULLFSYNC`, tens of ms
  per commit). Current stance is durable-to-OS-crash; genuine power cuts can
  lose the WAL tail (checksummed, so lost-not-torn). If rung-3 durability is
  ever wanted, the shape is a full-flush barrier before an operation's FIRST
  external upload, not per-commit fullfsync.

## Out of scope

Error-model tiers (#20–22), blob auto-evict (#25), migration authorizer (#26),
host-facade curation (#27). No data migration for pre-existing stores.
