# coven

## Contents

- [1. Overview](#1-overview)
- [2. Threat model](#2-threat-model)
- [3. Guarantees](#3-guarantees)
  - [3.1 IO bounds](#31-io-bounds)
- [4. Storage providers and access](#4-storage-providers-and-access)
- [5. Local database](#5-local-database)
- [6. Syncing writes](#6-syncing-writes)
  - [One sync pass](sync-pass.md)
- [7. Order](#7-order)
  - [7.1 Causality](#71-causality)
  - [7.2 Timestamps](#72-timestamps)
  - [7.3 Example](#73-example)
- [8. Merge](#8-merge)
  - [8.1 Example](#81-example)
  - [8.2 Concurrent writes to one row](#82-concurrent-writes-to-one-row)
  - [8.3 Deletes](#83-deletes)
  - [8.4 Foreign keys](#84-foreign-keys)
  - [8.5 Keys and uniqueness](#85-keys-and-uniqueness)
  - [8.6 CHECK constraints](#86-check-constraints)
  - [8.7 Triggers](#87-triggers)
  - [8.8 Owner tables](#88-owner-tables)
- [9. Members and roles](#9-members-and-roles)
- [10. Device identity](#10-device-identity)
- [11. Keys](#11-keys)
  - [11.1 Cryptography](#111-cryptography)
- [12. Joining and restore](#12-joining-and-restore)
  - [12.1 A person's new device](#121-a-persons-new-device)
  - [12.2 Adding a person](#122-adding-a-person)
  - [12.3 Losing everything](#123-losing-everything)
- [13. Removing members and devices](#13-removing-members-and-devices)
- [14. Audiences](#14-audiences)
  - [14.1 Roots and descendants](#141-roots-and-descendants)
  - [14.2 Moving rows](#142-moving-rows)
  - [14.3 Circles](#143-circles)
  - [14.4 Writes](#144-writes)
  - [14.5 References](#145-references)
  - [14.6 Leaving a circle](#146-leaving-a-circle)
  - [14.7 Deleting a circle](#147-deleting-a-circle)
- [15. Snapshots](#15-snapshots)
- [16. Files](#16-files)
  - [16.1 Kinds and where files are](#161-kinds-and-where-files-are)
  - [16.2 Storage and naming](#162-storage-and-naming)
  - [16.3 Reading ranges](#163-reading-ranges)
  - [16.4 Cache](#164-cache)
  - [16.5 Uploads and deletion](#165-uploads-and-deletion)
  - [16.6 What a device keeps about files](#166-what-a-device-keeps-about-files)
- [17. Schema changes](#17-schema-changes)
  - [17.1 Host application](#171-host-application)
  - [17.2 Coven's schema](#172-covens-schema)
- [18. Operations](#18-operations)
  - [18.1 Operations](#181-operations)
  - [18.2 Example](#182-example)
- [19. Recovery](#19-recovery)
  - [19.1 Noticing](#191-noticing)
  - [19.2 Recovering one device](#192-recovering-one-device)
  - [19.3 Resetting a store](#193-resetting-a-store)
- [20. Crates and conventions](#20-crates-and-conventions)
  - [20.1 Crates](#201-crates)
  - [20.2 Capabilities, owners and lifetimes](#202-capabilities-owners-and-lifetimes)
  - [20.3 Code conventions](#203-code-conventions)
  - [20.4 Checks](#204-checks)
- [Appendix A. SQLite behavior in synced tables](#appendix-a-sqlite-behavior-in-synced-tables)
  - [A.1 Integer primary keys](#a1-integer-primary-keys)
  - [A.2 Tables with no primary key](#a2-tables-with-no-primary-key)
  - [A.3 Unique constraints besides the primary key](#a3-unique-constraints-besides-the-primary-key)
  - [A.4 Restrict and no-action foreign keys](#a4-restrict-and-no-action-foreign-keys)
  - [A.5 CHECK constraints](#a5-check-constraints)
  - [A.6 Triggers that write synced tables](#a6-triggers-that-write-synced-tables)
  - [A.7 Schema changes](#a7-schema-changes)
  - [A.8 SET DEFAULT foreign keys](#a8-set-default-foreign-keys)
  - [A.9 References to non-key columns](#a9-references-to-non-key-columns)
- [Appendix B. Proof of convergence](proofs/merge.md), in its own file
- [Appendix C. Proof of the store log](proofs/storelog.md), in its own file
- [Store-log effects on data](proofs/storelog-data.md), coupling Appendices B and C
- [Appendix D. Storage format](format.md), in its own file
- [Appendix E. API](api.md), in its own file

## 1. Overview

- Sync for a small intimate group: a person's own devices, or a household.
- The app's data is SQLite. The app keeps its schema and writes ordinary SQL.
- Rows can carry files, like audio, images or documents, which sync with
  them.
- No server. It syncs through storage the members already have.
- Everything coven writes to the provider is encrypted: with the store key,
  a circle's key, or a member's public key.
- Only members' devices hold the store key.

## 2. Threat model

- The storage provider is assumed *honest but curious*, not *hostile*:
  - it may read what it stores;
  - it doesn't alter or withhold what it stores;
  - it sees object sizes, counts, timing, and which device writes.
- A thief with storage credentials but not the store key may be hostile.
- Deletion and withholding can't be prevented. No design can.
  - Coven does not recover lost or damaged storage. The household starts a
    new store by hand; the reset in §19.3 is for devices that disagree, not
    for rebuilding storage.
- Current members trust each other: each has access to the storage and
  isn't hostile on it.
- An ex-member may keep a copy of the store key they had.
- Each person keeps their restore code to themselves, on paper or shown on
  a screen ([§12](#12-joining-and-restore)); anyone who sees it can open
  the store with storage access while its keys are current. Anyone holding
  that current pair is that member to coven; coven cannot tell them apart.
  Replacement cannot revoke access to historical keys and ciphertext already
  available to that pair; old signatures retain §10's grace period. Theft of a restore
  code is out of scope: coven offers no recovery from a stolen code. The
  household removes the member and starts over by hand if needed.

The guarantees assume correct ciphers, signatures, hashes, key derivation,
random generation and secret custody ([§11.1](#111-cryptography)). No Lean
model proves their security: authentication and sealing are supplied inputs.
Atomic, durable SQLite transactions and durable file/custody writes are also
assumed. The provider behavior above and in §4, §10 and §13—including complete
reads, immutable publication, clock bounds, request duration and confirmed
access cut-off—is assumed, not established for real providers by the models.

## 3. Guarantees

Proof links below cover the stated model results under their hypotheses, not
the whole Rust implementation. Cryptographic and environmental assumptions
are those of [§2](#2-threat-model); the write-ups state their additional limits.

- **Availability:** the app reads local data and commits writes with no
  network; a commit never waits for sync. This does not promise that every
  write is accepted:
  - A write into an audience reloading after a reset, a schema raise requiring
    reload, or skipped history returns `DbError::AudienceReloading` until the
    reload commits (§14.4, §15, §19.3). This refusal is an exception to write
    availability, not a wait inside the write call. Other audiences remain
    writable; an ordinary snapshot reload permits app writes throughout (§15).
  - `NotInCircle` and `DeletedCircle` refuse writes using the locally applied
    membership/deletion state; `ReferenceAudience` refuses a reference or move
    across an unreadable audience, and `KeyNotUuid` refuses an invalid independent
    row key (§8.5, §14.5–§14.7). None requests a network check at commit.
  - Schema declarations and migration/version errors gate opening or migration
    (§8, §14.1, §17). SQL constraints and trigger failures can reject a write;
    `InternalTable`, `StatementForbidden`, `TriggerTarget` and `FileColumnWrite`
    enforce local SQL access rules. These are local validity checks, not waits
    for schema publication. A migrated app's writes can commit before the raise
    is published; publication gates upload (§17.1).
  - Self-replacement pauses app writes with `MemberKeysChanging` while this
    installation confirms its queue and installs its new binding/custody (§10).
  - `ClockOutOfRange` and `TooLarge` enforce timestamp and representation limits
    (§6, §7.2). File attachment checks can reject absent/changed sources, wrong
    sizes or references, missing bytes, invalid file declarations/keys/columns,
    reused or unreferenced staged files, or replacement of a write-once file
    ([E3](api.md#e3-writing)). SQLite/disk failures, `TransactionEnded` and
    rollback failures are local failures too; a closed or unusable installation
    cannot accept writes (§10, [E1](api.md#e1-opening)). A post-commit file-cleanup
    failure preserves and reports the committed outcome (§16.6).
  Uncached file reads require storage (§16.3); administrative calls require
  online catch-up (§9). The [queue model's `reloading_blocks_commit`](proofs/queue.md#availability-and-commit-refusals)
  proves the reload gate. Commit has no network precondition in its transition
  definition, but no separate availability theorem or proof of Rust's lack of
  network waits is provided.
- **Confidentiality:** members' data can't be read by those it isn't for:
  - the provider and outsiders can't read any of it — assumed cryptography,
    not proved in Lean;
  - nothing the provider sees is computed from content without a secret
    key, so it can't test whether you store a file or value it knows — assumed
    cryptography and correct use of §11.1, not proved in Lean;
  - a circle's contents are readable only by its members
    ([§14](#14-audiences)). Secrecy assumes cryptography; the
    [key-access and revocation results](proofs/storelog-data.md#results-and-their-assumptions)
    prove conditions on key receipts, with §11's historical disclosures and
    observation window, not encryption security.
- **Integrity:** members' data can't be tampered with:
  - members: no member can write as another member — assumed signature
    security; [store-log authority checks](proofs/storelog.md#c9-what-lean-checks-and-what-is-prose)
    start with an authenticated author;
  - outsiders: nobody without the store key can alter or forge it — assumed
    authenticated encryption, signatures and key derivation, not proved in Lean.
- **Authorization:** only admins ([§9](#9-members-and-roles)) change
  membership, and every device enforces it. The [store-log model](proofs/storelog.md#c7-invariants)
  proves the recorded-past authority checks given authenticated authors;
  [current replay](proofs/storelog.md#c11-key-free-removals-and-circles-left-empty-after-replay)
  uses those checks. Signature security and implementation enforcement are assumed.
- **Atomicity:** another device applies a write in one transaction, so a
  write is never half visible
  ([queue proof: `AtomicReceive.never_half_visible`](proofs/queue.md#what-is-proved)).
  This is proved at the transaction boundary; SQLite's atomic commit and rollback
  are assumed, not proved.
- **Convergence:** every device ends up with the same data:
  - a write made after seeing another wins over it on every device
    ([merge: `write_after_read_wins`](proofs/merge.md#b6-proof-for-the-merged-state));
  - no device consumes a write before the positions its author had read — the
    [receive proofs](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data)
    derive causal application from receive steps, distinguishing consumed
    positions from retained effects. An otherwise eligible change whose
    generation source or replaced setter was dropped is recorded as a frozen
    loss of the original change (§8); its write's unrelated changes apply.
    A directly dropped retirement write or reset-ignored part has no effects
    or losses. The [queue verdict proofs](proofs/queue.md#what-is-proved)
    connect the same per-change rule to schema conversion and row merge;
  - when a concurrent write replaces or deletes a value, that value is
    recorded, not silently dropped. Values computed by a breaking migration
    that lost to a concurrent one are replaced by the winner's (§17.1).
    [Merge's value accounting](proofs/merge.md#b14-where-every-value-goes) proves
    these dispositions for valid inputs and supplied boundary verdicts.
  [Merge convergence](proofs/merge.md#b8-proof-for-the-removal-rules) and
  [store-log/data convergence](proofs/storelog-data.md#results-and-their-assumptions)
  require the same eligible, readable inputs; eventual receipt additionally
  needs successful [discovery](proofs/io.md#discovery-obligations), keys and readers.
- **Durability:** committed writes and recorded operation progress survive
  a crash while this device's database and identity remain usable
  ([queue proof: `crash_keeps_committed`, `Operations.crash_preserves_operation`](proofs/queue.md#what-is-proved)):
  - every committed write is eventually uploaded if syncing continues,
    prerequisites and fair progress let each waiting head reach its first
    attempt, and storage acceptance and local acknowledgement eventually succeed
    ([queue proof: `every_queued_write_settles`](proofs/queue.md#what-is-proved));
  - a damaged local database or restored stale copy starts again from
    storage. Edits it had not uploaded are lost, and the app is told
    (§10, §19.2). The [storage model](proofs/storage.md#restore-another-live-copy-and-writes)
    checks the loss and reset boundary, not successful recovery or app notification;
  - a recorded rotation or snapshot publication resumes with the same data
    within one attempt, and finishes under §18's prerequisites and fair
    progress assumptions
    ([queue proof: `rotation_resumes_and_finishes`, `snapshot_resumes_and_finishes`](proofs/queue.md#what-is-proved)).
    Retention separately deletes the history that snapshots safely cover.
  - if replay drops the attempt's entry and work remains, §18 starts over
    from the first step with a new entry number and, for a rotation, a fresh
    key. Old copies and the entry remain in storage
    ([checked history: `restart_changes_key`](proofs/queue.md#checked-histories-and-limits)).
    A rejected member-key replacement reports its reason without restarting;
    overriding the winner requires a new explicit call or approved invite (§18).
    Finishing does not promise recovery from permanent rejection without
    the applicable update or app action (§19.1).
  These model results assume durable local artifacts and atomic journal commits;
  their progress assumptions are stated above and in §18.
- **Revocation:** before sending a write for the first time, a device
  catches up on membership changes and new sealed key copies at sync-pass
  start, then seals with the newest usable key. Every send also requires
  that completed catch-up to have started less than five minutes ago (§10).
  For an audience the sending member still belongs to, selection excludes
  keys known to have reached an excluded member or a retired or noncurrent
  sealing credential (§11)
  ([queue proof: `first_key_excludes_known_recipient`](proofs/queue.md#what-is-proved),
  [credential exposure proof](proofs/storelog-data.md#member-key-replacement)).
  An excluded member cannot read writes first sent by a remaining audience
  member's device that already knew of their exclusion, provided no copy of
  the sealing key is made for them after that pass's copy observation (§11;
  [queue proof: `first_key_never_delivered`](proofs/queue.md#what-is-proved)).
  [Store-log/data revocation](proofs/storelog-data.md#results-and-their-assumptions)
  proves absence of a key copy under that window hypothesis; unreadability
  additionally assumes correct cryptography. The [IO send-gate proofs](proofs/io.md#requests-and-retained-observations)
  check the catch-up deadline given the clock and observation inputs.
  - A tried write retains its first attempt's key and bytes on every retry
    ([queue proof: `Run.retries_identical`](proofs/queue.md#what-is-proved)).
    Retries cannot rewrite those bytes; replaced signing credentials stop
    sending under §10, and receivers enforce each object's window. Storage
    access is cut off separately (§13). The retry proof assumes a deterministic sealer;
    provider cut-off is assumed.
  - Replacing member keys preserves the member id. Its previous sealing
    credential and losing candidates' copies expose audience keys even
    while the member remains active. Selection avoids those observed copies;
    later copies retain §11's residual window. Old signing-key admission has
    a grace period, not immediate revocation or theft recovery. Its window
    becomes final with a kept replacement and its final preceding prefix,
    including ties; the keyed proofs assume that prefix (§9, §10).
  - The removed member's own pre-removal circle writes are outside this
    promise (§14.6). E.g. Ben queues a Gifts edit, then learns Ana removed
    him. He may still first-send that edit with the old key he already has;
    it exposes no new circle data to him. Ana, still a member, selects the
    replacement key for her first attempts
    ([checked histories: `departed_member_counterexample`, `remaining_member_rotates`](proofs/queue.md#checked-histories-and-limits)).
- **Bounded storage:** §15 bounds coverage separately for each audience;
  deleting a write object requires every one of its part audiences, because
  its parts are stored together
  ([`audience_deadline`, `bounded_storage`](proofs/io/CovenIO/AudienceRetention.lean)).
  - Let `G` be a storage time from which the write is final and passes the
    reader-positions or 30-day age gate, `W` its first stored time, and `O_a`
    the time a reader of audience `a` can supply the successful services
    below ([`AudienceRetention.ready`](proofs/io/CovenIO/AudienceRetention.lean)).
  - Retention evaluates candidates every pass and requests missing coverage
    in that pass or the next; let `P` bound pass spacing, `U` bound complete
    successful counting publication, and `D` bound successful deletion by
    an authorized owner
    ([`bounded_storage`](proofs/io/CovenIO/AudienceRetention.lean)).
  - `U` includes catch-up, preparation, failed or abandoned attempts and
    confirmation, not merely one upload request: a busy log's eventual
    terminal miss is a completion hypothesis, not a consequence of the
    publication rule
    ([`first_witness`](proofs/io/CovenIO/AudienceRetention.lean)).
  - For each part audience, put `H_a = max(G, O_a, W + 1)` and
    `B_a = H_a + 2P + U`: a counting covering snapshot lands by `B_a`, even
    if that audience began with no snapshot
    ([`first_witness`](proofs/io/CovenIO/AudienceRetention.lean)).
  - Put `R_a = B_a + one day + 1`: by `R_a + 2P + U`, that audience has
    both mature historical coverage and current newest counting coverage,
    assuming the same bounded service refreshes missing current coverage
    ([`audience_deadline`](proofs/io/CovenIO/AudienceRetention.lean)).
  - For physical deletion, take `B = max_a B_a` over all part audiences and
    `R = max(G, B + one day + 1)`; the write is deleted by
    `R + 3P + U + D`, with successful coverage services for every part,
    retention scheduled within `P` after coverage, and deletion completing
    within `D`
    ([`bounded_storage`](proofs/io/CovenIO/AudienceRetention.lean)).
  - These are the model's integer-second deadlines: `W + 1` makes the
    write strictly older than catch-up, and `B + one day + 1` makes its
    witness strictly more than a day old; compare actual provider times
    with the strict inequalities, preserving their precision (D2)
    ([`first_witness`, `audience_deadline`](proofs/io/CovenIO/AudienceRetention.lean)).
  - The bounds assume at least one part, sound publication and deletion
    histories, normalized positions over the known writer inventory, retained historical
    evidence, and service durations on the same storage-time scale; they
    concern write objects, not snapshot objects, files or a total byte
    bound at an unbounded write rate
    ([`bounded_storage`](proofs/io/CovenIO/AudienceRetention.lean);
    provider execution and durable evidence are implementation obligations).
  - If covering witnesses already landed by `B`, the same
    `R + 3P + U + D` bound applies without waiting for first publication;
    if current coverage already holds when mature and gated at `R`, the
    bound is `R + P + D`
    ([`bounded_witnessed_storage`](proofs/io/CovenIO/RetentionDeadline.lean),
    [`scheduled_deletion_bound`](proofs/io/CovenIO/Reload.lean)).
  - The finality gate uses §9's endpoint `T′ = T - M - δ`, with the
    same `M = one hour` for every provider. G above includes that delay.
    Increasing a gate from `G₀` to `G₀ + M` increases each deadline by at
    most M, with the same reader and successful-service assumptions:
    `H_a = max(G₀ + M, O_a, W + 1)` and `R = max(G₀ + M, B + one day + 1)`,
    with B derived from those shifted `H_a` values. Snapshot maturity
    still uses its own landing time
    ([shifted retention bounds](proofs/io.md#discovery-obligations)).
  - E.g. the checked initially unsnapshotted write becomes gated and has a
    reader on day 30, with one-second pass spacing and immediate successful
    publication and deletion: the theorem bounds deletion by day 31 plus
    six seconds
    ([`first_snapshot_deadline`](proofs/io/CovenIO/CommonReloadExamples.lean)).
  - An audience with missing coverage and no available reader has no such
    bound, nor does a write object shared with that audience; eventual
    return supplies a deadline relative to `O_a`, not one fixed from `G`
    ([`offline_reachable`](proofs/io/CovenIO/CommonReloadExamples.lean),
    [`first_witness`](proofs/io/CovenIO/AudienceRetention.lean)).
  - E.g. Ana uploads one write with store and Gifts parts, then goes
    offline; Ben can snapshot the store but cannot read Gifts, so every
    deadline can pass without permitting deletion of that shared object
    ([`offlinePart`](proofs/io/CovenIO/CommonReloadExamples.lean)).
  - Already mature current coverage can permit deletion while its readers
    are offline, provided the authorized deletion owner runs
    ([`scheduled_deletion_bound`](proofs/io/CovenIO/Reload.lean)).
  - Superseded snapshots follow §15's separate deletion rules; the store
    log, sealed keys and still-required boundary snapshots remain.
  - An absent device does not hold finality back: racing admin decisions
    settle once the provider sample T is more than 30 storage days plus
    one hour plus δ after the last late entry, given completed discovery
    under §4's listing requirement (§9;
    [`quiet_after_last_late`](proofs/io/CovenIO/ListingFinality.lean)).
- **Nothing waits silently:** whatever coven cannot apply or deliver is in
  the pending list, with its subject and typed reason (§19.1).
  [Store-log/data pending proofs](proofs/storelog-data.md#results-and-their-assumptions)
  and [IO download proofs](proofs/io.md#downloads-and-their-visible-outcomes)
  cover modeled subjects; complete failure reporting by Rust is not proved.

### 3.1 IO bounds

The request order and cache lifetimes are in [One sync pass](sync-pass.md).
These are requirements on a device's sync, not estimates for a particular
store. Discovery reads each undrained writer's next number (§6). Idle request counts
depend on devices, not accumulated history. All read, transfer, retry and
local work limits below apply without a change feed or history listing.
The confidentiality, integrity, finality and atomicity rules still apply.
The [IO model](proofs/io.md#requests-and-retained-observations) proves the
modeled request, reuse and wait bounds under its stated inputs. Native provider
requests and local implementation work remain [verification obligations](proofs/io.md#rust-checks-outside-the-model),
not Lean-proved bounds on Rust.

A *steady store* has a complete local catalog, unchanged storage and local
work, no evicted input needed by this pass, and no due retry, finality,
retirement drain or retention deadline. Its connection and credentials are
usable. Starting with an empty catalog and rebuilding an evicted cache are
measured separately as initialization or eviction work.
Repeatedly discarding a catalog at pass end is not eviction.

Count actual provider requests, including pagination, metadata lookups,
redirects, upload-session calls and SDK retries. A logical `list` call that
fetches three pages counts as three requests. Count received metadata as well
as object bodies in downloaded bytes. Also report body bytes separately, so
a zero-body pass cannot hide a listing of the whole history.

- **Request accounting:** separate discovery `D`, object reads `O`, transfer
  parts/ranges `F`, produced objects `P`, deletions `X`, and provider access
  work `A`. Also count `M`, the metadata lookups, redirects, session-control
  calls and confirmations not included in another term, and `E`, failed or
  repeated requests. No request belongs to two terms.
  - `D` counts discovery listing pages and terminal next-number misses.
    A successful next-number read belongs to `O`, not also to `D`.
    Folder traversal and exact-name lookup overhead belong to `M`, except
    when the lookup itself establishes the miss already counted in `D`.
    `O` counts distinct object versions whose headers or bodies are needed.
    At most two header ranges and one remaining-body stream are allowed
    per object without failures or eviction: at most `3 * O` requests.
  - `F` counts every file range, upload part and native asset-part read;
    these are excluded from the object-stream count. `P` counts each
    create or replacement publication request; session begin/status/finish
    requests belong to `M`, and their parts to `F`.
  - `X` counts deletion requests, including native deletion of asset parts.
    `A` counts permission reads, pages, grants, revokes and due job polls.
    `M` includes fresh single-object status and occupied-path comparison
    overhead. A required complete-byte comparison or snapshot verification
    body instead contributes an object to `O`.
  - Thus `R <= D + 3 * O + F + P + X + A + M + E`. Initialization and
    eviction are reported separately with the same terms. An arrival that
    causes a snapshot or historical-key sharing contributes to `P`, not an
    unspecified constant per arrival. A file's length contributes to `F`.
- **Idle requests:** `O = F = P = X = A = E = 0`, so `R = D + M`.
  S3 positions listings require one HEAD per listed object to obtain its
  replacement revision: `ListObjectsV2` omits versions and user metadata.
  These HEADs contribute to `M`; other providers return revisions in listings.
  List store-log device folders and `positions/`, and make one next-number
  miss per polled device for each of three logs: store-log entries, writes
  and key copies. `N` is active devices plus retired devices not yet drained
  (§6). With each listing fitting one page,
  `D = 2 + 3 * N` on every provider. Folder ids are already known in this
  warm count. All extra pages, initialization lookups and retries are charged.
  Drive's unknown-name miss is one exact parent/name query; a hit needs
  that query, exact metadata and the download (§4). Metadata requests and
  replacement rechecks contribute to `M`. Neither lists historical log objects.
- **New objects:** checked immutable facts and retained bytes are reused.
  Charge only newly needed versions to `O`; changed positions count as new
  versions. Work induced by them stays in its own term above.
- **Downloads:** each immutable object is downloaded at most once per
  device while its retained bytes or sufficient checked facts remain.
  Reopening, another consumer, a missing prerequisite, or another pass does
  not justify another download. For replaceable positions this means once
  per observed version; for range-read files, once per retained chunk.
  A partial failed transfer is not a completed download: retry its missing
  bytes where supported and charge every retransmitted byte separately.
  Eviction permits another fetch, which is counted as eviction work.
- **Bytes:** report discovery metadata separately from new header, body and
  file-range bytes. Complete listings can repeat unchanged metadata; body
  reads cannot repeat unchanged retained bytes. A newly needed range is
  charged once. Prefix inspection uses at most two range reads. Loading fetches the
  uncached remainder in one body stream, reusing the retained prefix
  ([One sync pass](sync-pass.md#one-stream-checked-as-it-arrives)).
- **Waiting:** every automatic wait has a bounded request rate, including
  operations, invites, joining, missing keys and provider throttling.
  Delays are 1, 2, 4, …, 256, then 300 seconds between unsuccessful attempts,
  measured monotonically. Unsuccessful includes a successful request that
  finds its prerequisite still absent. A provider's `Retry-After` can only
  lengthen them.
  For `W` unchanged waits in one uninterrupted run, at most
  `W * (1 + floor(t / 1 second))` attempts start in any interval of length
  `t`; after reaching the cap it is `W * (1 + floor(t / 300 seconds))`.
  Multiply by each wait's declared request allowance, counting pages and
  retries within it. Neither pass completion nor a one-second worker tick
  resets a delay. Reopen uses the persisted T and W in §16.5. Pages are
  charged to discovery; the restart rate bound assumes no forward clock
  jump that makes a still-live wall-clock deadline appear expired.

Local work has bounds too:

- Opening unlocks store-key custody and member-key custody once each for
  the handle's session. A warm pass performs zero unlocks or passphrase
  derivations. Persist only changed keys through the same unlocked custody
  session; persistence does not derive the passphrase key again.
- Decode the saved store log at most once when the sync owner starts, on a
  read connection; keep the decoded value between passes. A warm idle pass
  performs zero full-log decodes and zero replays. Decode arriving entries
  once and replay each causally ready batch once; author-view checks remain
  distinct from the replay of the resulting received set (§9).
- An idle pass takes the database writer zero times. No writer connection
  is held across a storage request, custody call, timer wait, whole-object
  decoding, or store-log replay. Read-only decisions use read connections;
  a writer is held only to validate and commit actual state changes. An
  atomic apply can include its database work; it cannot include a download.
  Each transaction acquires the writer once; count failed transaction
  attempts separately rather than hiding repeated holds in a success count.
- An unchanged idle pass commits zero durable transactions.
  No unchanged operation rows, pending records or last-checked timestamps
  are rewritten. Completion status is an in-memory notification (E5).
- Hot lookups use indexes: object identity; write audience and position;
  snapshot coverage; peer device and observed version; references by file
  path and retaining object; pending subject/reporter and retry reason;
  store-log storage time; key introductions and observed copies; pending uploads and operations.
  Eager-file work selects changed references lacking complete cached bytes,
  rather than scanning every app row on every pass. Cache budget checks
  use a maintained namespace total, not a sum over every cached chunk.

Tests must count both logical operations and provider requests, transferred
bytes, custody calls, full-log decodes, writer holds and durable commits.
Run the same idle and `k`-arrival cases with more unchanged history, more
files, more devices and enough objects to cross listing pages. Assert hot
query plans with representative populated tables: a scan of unchanged app
or history rows is not an indexed lookup. Exercise warm passes, reopening,
cache eviction, interrupted streams, withheld prerequisites and rate limits
separately. Measure each term above; a logical call never hides native work.

**Ana's quiet library.** Adding 20,000 old files changes neither her idle
requests, body downloads (zero), nor local durable work (zero). With Ana's
phone, Ben's laptop and Carol's tablet on S3, a pass is two LISTs plus nine
GET misses: **11 requests**. Ben's single new note can trigger a snapshot and an
eager file download: count the note under `O`, the snapshot under `P`, and
the file's ranges under `F`.

## 4. Storage providers and access

- A store lives on one provider: S3, Google Drive, Dropbox, OneDrive, or
  iCloud through CloudKit.
- CloudKit is built into `coven-storage` on Apple platforms, using
  `objc2-cloud-kit` and `block2` completion handlers awaited through a oneshot
  channel. The app supplies its container identifier and the iCloud entitlement
  for that container. Other platforms refuse CloudKit at setup with
  `StorageFailure::ProviderUnavailable`; they never panic or require app code
  implementing provider calls.
  - Setup takes the container identifier, obtains the current user's record id,
    and creates the store's custom zone with
    [CKModifyRecordZonesOperation](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordzonesoperation).
    The recorded location retains container, owner and zone for restore and joins.
    Coven uses that zone in the owner's private database or the participant's
    shared database. Its object record names are the complete
    canonical D10 paths, including the store id. These ASCII names fit
    [CKRecord.ID's 255-character limit](https://developer.apple.com/documentation/cloudkit/ckrecord/id).
  - Create-once saves a **new** record with that id using
    [ifServerRecordUnchanged](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordsoperation/recordsavepolicy/ifserverrecordunchanged).
    An occupied name returns `CKError.serverRecordChanged`, mapped to
    `AlreadyExists`; coven never merges or overwrites that immutable record.
    Positions and clock objects instead use
    [allKeys](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordsoperation/recordsavepolicy/allkeys)
    for replacement; conditional replacement retains the fetched change tag
    and uses `ifServerRecordUnchanged`.
  - The publication record's server-assigned
    [creationDate](https://developer.apple.com/documentation/cloudkit/ckrecord/creationdate)
    is an immutable object's complete-publication time. Replacements use
    [modificationDate](https://developer.apple.com/documentation/cloudkit/ckrecord/modificationdate).
    [recordChangeTag](https://developer.apple.com/documentation/cloudkit/ckrecord/recordchangetag)
    supplies revision identity and changes on every save, including equal-byte
    replacements. Missing size, date or revision is a protocol error.
  - [CKQuery](https://developer.apple.com/documentation/cloudkit/ckquery) uses
    equality on the exact directory named by the requested prefix, in the
    configured zone, on `CovenObject`. Each publication atomically stores indexed
    string fields `directory1` through `directory4` for its existing ancestor
    directories, without trailing slashes: store, store/kind, store/kind/scope,
    and the snapshot writer directory. A prefix selects the field at its depth,
    so even `all`, `store_logs` and `snapshots` each use one equality query.
    CloudKit limits prefix-filter values to 30 characters, shorter than a store
    UUID; directory equality avoids that limit. Store-log records also index
    `number` as a decimal string. Writer discovery queries
    `directory2 == "<store>/store-log" AND number == "1"`, requesting only
    `path`. Object metadata queries request only `path` and `size`; both exclude
    assets and follow every
    [CKQueryOperation cursor](https://developer.apple.com/documentation/cloudkit/ckqueryoperation).
    The common listing margin is `M = 1 hour`: a query at storage time `T`
    establishes completeness only for publications before `T - M`. This is
    coven's provider assumption, not an Apple guarantee of an indexing deadline.
    Exact-id fetches establish status; only `CKError.unknownItem` means absent,
    while `zoneNotFound` means `ContainerNotFound`.
  - [CKAsset](https://developer.apple.com/documentation/cloudkit/ckasset) carries
    encrypted bytes. Saving a record saves its assets too; coven never exposes
    an incomplete asset as a complete object. Bounded asset parts support ranges
    without downloading unrelated parts. CloudKit has no application-controlled
    resumable byte upload: a recorded upload is coven's durable session id,
    destination, total and confirmed part boundary. Each asset part and its
    confirmed boundary are saved together in the custom zone using
    [atomic modification](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordsoperation/isatomic).
    Resumption fetches that session by id, including after a lost reply, and
    retains every confirmed part (§16.5).
  - Large-object publication creates the destination record only after all
    parts exist, atomically marking the session published. Its `creationDate`
    therefore dates complete publication, never session creation or an early
    part. Pending sessions and parts use separate record types and reserved
    names outside D10; listings cannot mistake them for published objects.
    The session id identifies a lost successful publication reply. Abort
    preserves published objects; deletion removes the publication and its
    owned parts. CloudKit's
    [deleteSelf references](https://developer.apple.com/documentation/cloudkit/ckrecord/reference)
    express ownership of those parts. Native atomic saves need no conditional
    server-side copy to hide partial record uploads.
  - Account sharing uses a private, read/write
    [CKShare for the configured zone](https://developer.apple.com/documentation/cloudkit/ckshare).
    Only the owner changes participants. Coven fetches the participant's native
    identity, saves the share and puts its server-issued URL in the encrypted
    invite. Joining fetches
    [share metadata](https://developer.apple.com/documentation/cloudkit/ckfetchsharemetadataoperation),
    verifies container, owner and zone, then calls
    [CKAcceptSharesOperation](https://developer.apple.com/documentation/cloudkit/ckacceptsharesoperation).
    Removing a member removes only that participant and saves the share;
    owner, public or otherwise nonexclusive access is retained and reported.
    `notAuthenticated`, `permissionFailure` and `quotaExceeded` preserve their
    native causes as authentication, permission and quota failures respectively.
    `partialFailure` is interpreted through `CKPartialErrorsByItemIDKey`, never
    as success; `CKErrorRetryAfterKey` supplies the provider's retry delay.
- Every member reaches the storage using their own provider account.
- Access starts at the least privilege that supports the store. Full-account
  access exposes unrelated files and requires provider review; an app offering
  only one-person stores need not register it.
  - **Google Drive:** every new store requests
    `https://www.googleapis.com/auth/drive.file`. The same person can use the
    store across devices and across client ids in the app's Google Cloud project.
    This permission does not let another person read the creator's files, even
    after the folder is shared with them.
  - At the first invite, coven requests
    `https://www.googleapis.com/auth/drive` on the same client, with
    `include_granted_scopes=true`. Only a confirmed full grant permits the
    sharing request. Existing files remain in place with the same ids, bytes,
    timestamps and revisions. Cancellation, denial or an insufficient grant
    fails the invite without granting the recipient access.
  - The app declares whether it registered full Drive access. A client registered
    only for `drive.file` receives a typed missing-access error from
    `create_invite` before any provider change. Joining a shared Drive store
    requires full Drive access too. Full `drive` requires Google's restricted-scope
    review; `drive.file` avoids that review.
  - A restore initially requests `drive.file`. A shared folder visible under that
    grant reports that full access is required; a folder returning 404 reports
    that it is not visible under this grant, preserving the provider cause. The
    app requests full access and retries. A narrow grant cannot establish that
    another person's shared folder is absent.
  - E.g. Ana creates Notes with `drive.file` and opens it on her laptop. Inviting
    Carol asks Ana for full Drive access; accepting lets coven share the existing
    folder. Carol signs in with full Drive access to read Ana's files.
  - **Dropbox:** the app registers an App-folder key, a Full Dropbox key, or both.
    The access level is fixed when the store is created on Dropbox. With both
    keys, creation requires an explicit shared/not-shared choice, with no default.
    With one key, that key determines the access level. A Full Dropbox store can
    be shared even if it initially has only one person.
  - An App-folder app addresses paths relative to its own folder, without a
    shared-namespace path-root header. Dropbox refuses sharing inside that
    folder: `create_invite` returns a typed error before changing the provider.
    Sharing such a store later requires relocating it, which is outside this
    access-level operation. Moving Dropbox files changes their server timestamps
    and revisions, so switching keys is not an in-place access upgrade.
    Full Dropbox apps require production review to serve beyond development use.
  - E.g. Ana chooses not-shared for Notes and coven uses the App-folder key.
    Inviting Carol reports that Notes needs relocation. Choosing shared when
    creating Household uses the Full Dropbox key and permits later invitations.
  - OneDrive, CloudKit and S3 retain their provider access rules below.
- For Google Drive, Dropbox and OneDrive, the app triggers `authenticate` on
  the builder or open handle. Coven builds the sign-in request, checks the
  redirect, exchanges the code and holds the tokens; none pass through the app.
  - The builder takes one presenter: open this authorization URL and return the
    provider's redirect or cancellation. Coven supplies the desktop browser and
    local listener; iOS and Android apps supply their platform sign-in sheet.
  - Before storage setup or bootstrap publishes a store, the sign-in lives in
    coven's session custody. Publication commits it to device custody. Coven
    uses a token until the provider rejects it with HTTP 401. It then
    refreshes once, commits the replacement and retries that request once.
  - Token expiry uses no device clock. Refresh failure or a second rejection
    reaches the caller; every storage consumer uses the same rule.
  - E.g. Ben sets his laptop clock back a year. Its token still works until
    the provider rejects it; one refresh lets the waiting request continue.
- On S3, each member has their own access key.
  - An S3 service's behaviors are data, the public `S3Settings`
    ([E5](api.md#e5-storage-and-sync)), one enum value each: AWS SigV4 or
    Google HMAC signing; the single-PUT create-once condition
    (`If-None-Match: *` or `x-goog-if-generation-match: 0`); multipart
    publication by conditional completion, or by copy with a destination
    condition (`If-None-Match: *`, `x-goog-if-generation-match: 0` or
    `cf-copy-destination-if-none-match: *`); the native multipart timestamp
    (completion or start); the checksum policy (scope stated, unstated scope
    ignored, or none returned); listing V2 or V1; ETag or generation revision;
    S3 or Google response headers; and
    whether reads request checksums. Each behavior is implemented once in the
    adapter; apps do not supply capability flags or service branches.
  - A location's profile is one of three: none, selecting by endpoint host; a
    catalogued service's name; or a custom profile carrying its own complete
    `S3Settings`. The location records it as given, so restore codes and every
    other device use identical behavior.
  - Catalogued services are const entries in the adapter's
    [`s3_profile.rs`](../crates/coven-storage/src/providers/s3_profile.rs):
    name, endpoint host or host suffix (none for a self-hosted service), the
    settings they fill, the finest scope a member key can have (below), the
    date and suite version of their last certification, and separate
    certification evidence for versioned buckets. Versioning support belongs
    to the service profile, never to `S3Settings`.
  - No endpoint selects **AWS S3**. Host suffix `.amazonaws.com` selects AWS;
    exact host `storage.googleapis.com` selects **Google Cloud Storage (GCS)**
    through its XML API; suffix `.r2.cloudflarestorage.com` selects
    **Cloudflare R2**, including jurisdiction-specific account hosts. Suffixes
    include the separating dot and require a preceding host component.
  - Self-hosted services have arbitrary endpoints, so no hostname identifies
    them. A named profile is used as is. A name or custom profile never
    overrides a catalogued host: such an endpoint admits only its own
    profile's name. Without an endpoint, requests reach AWS, so only AWS may
    be named and no custom profile is accepted. Catalogued hosts require
    `https`; other endpoints may use plain `http`, as a self-hosted server on
    the same machine or a private network can. A contradicting name or custom
    profile, or a plain-`http` catalogued host, fails validation with
    `InvalidConfiguration` before any request. The self-hosted profiles are
    **RustFS**, **SeaweedFS** and **Ceph RGW**; no hostname selects them.
  - **Member key scope.** A service limits a member key either to a prefix
    inside a bucket or only to a whole bucket. AWS (IAM `s3:prefix`
    conditions), GCS (a managed folder per store), RustFS, SeaweedFS and Ceph RGW
    (policy `s3:prefix` conditions) limit it to the store's prefix, so their
    stores can share a bucket and base prefix. An R2 API token is limited to
    buckets, never to a folder inside one, so every member's key reaches
    every object in its bucket: a member of one store could delete another
    store's ciphertext there. On such a service every store has its own
    bucket from creation, however its keys are made (by hand in the
    provider's dashboard, or by coven). A custom profile, detected or chosen,
    says nothing about keys, so it takes bucket scope too.
    - The location's base prefix is empty. Any other prefix fails
      validation before any request with
      `StorageFailure::OwnBucketRequired { reason: Prefix }`.
    - A store's first setup, before any location is recorded, lists the
      bucket's root with delimiter `/` before detection or any other write.
      Any object or folder other than the store's own `<store>/` refuses
      setup with `OwnBucketRequired { reason: Occupied }`. A retried creation
      that already wrote under `<store>/` passes. The listing margin M
      applies: an object stored within M may be absent, so two stores
      created in one bucket within M of each other can both pass.
    - Reconnecting, restore and joining use the recorded location and check
      no objects outside the store's own folder.
    - Moving a store into its own bucket at its first invite would copy all
      its data, so the rule applies from creation, including to one-member
      stores.
    - E.g. Ana creates Household in the R2 bucket `household`. Creating
      Garden in that bucket is refused; Garden gets the bucket `garden`.
  - An endpoint that matches no catalogued host and names no profile has no
    AWS fallback. Setup detects its behaviors (below) and records them as the
    location's custom profile; the returned and recorded location carries
    those settings. Connecting such a location outside setup fails with
    `InvalidConfiguration`. Reconnecting this device with the same location
    and no profile reuses the recorded location's settings, which every other
    device already uses, rather than detecting again.
  - **Detection** is a production operation the app may also run before setup
    to prefill advanced settings
    ([`detect_s3_settings`](api.md#e5-storage-and-sync)). It runs native
    probes against the user's bucket with the supplied key, writing only fresh
    objects under the store's provider-check prefix
    `<prefix>/<store>/.coven-check/<run>/`, where `<run>` is a fresh
    coven-generated UUID. This reserved namespace is distinct from every D10
    path; S3 listings exclude it, so a probe left behind by a killed process
    never reaches a published-object listing. Detection deletes its objects
    and aborts its multipart uploads afterwards, also after a failed probe;
    a failed cleanup fails the call, retaining both causes. The result is the
    detected `S3Settings` and one typed observation per probe, each saying
    whether the behavior held, with a native outcome that names no key,
    endpoint or object. The probes:
    - both signers, with a read-only zero-entry listing;
    - both single-PUT conditions: the first PUT must be accepted, a second
      refused as `AlreadyExists`, leaving the original bytes and time;
    - an unfinished multipart upload, which must be absent from HEAD, GET and
      listing;
    - conditional completion, accepted on a free name and refused on an
      occupied one with bytes, ETag and time unchanged;
    - the native multipart timestamp, bracketed by marker objects written
      before and after initiation and before and after completion;
    - each copy destination condition, accepted on a free name and refused on
      an occupied one with bytes and time unchanged, and whether the copy's
      time is later than its source's;
    - V2 against paginated V1 listing of the probe objects;
    - checksum mode, returned checksums and their scope, Google generation
      and hash headers, and a zero-entry listing at a missing object's key.
  - Detection pauses only where elapsed time distinguishes outcomes. Times
    are whole-second `Last-Modified` values, so it waits 1 second after the
    marker written after initiation, then rewrites a marker, pausing 250 ms
    between attempts, until the server dates it a later second than that
    marker; completion follows. A completion dated in the initiation bracket
    is a start timestamp. The copy probes run after that wait, so a copy dated
    later than its source is the copy's own time. Listing probes poll
    immediately and after each 250 ms pause until the listing shows every
    probe object. Both stop after 40 pauses, 10 seconds of waiting, beyond the
    worst listing lag measured (5.5 seconds): a marker that never advances
    fails detection, and a listing that never completes is a failed listing
    observation. Pauses use the store's clock capability. No other probe
    waits.
  - Detected settings take the first enforced single-PUT condition in the
    order above; conditional completion when it is enforced, unfinished
    uploads are hidden and multipart times date completion; otherwise the
    first enforced copy condition in the order `x-goog-if-generation-match`,
    `If-None-Match`, `cf-copy-destination-if-none-match`; the observed
    multipart timestamp; no checksum policy when none was returned on any
    probe, unstated-scope ignoring when one arrived without its scope, and
    stated scope otherwise; checksum requests only when checksum mode is
    accepted and checksums do not arrive without it; generation revision and
    Google headers when GCS's headers arrive; and V2 when it agrees with V1.
    Where no behavior holds, the setting keeps the plain
    S3 value and the setup check below names the failure.
  - **Versioned buckets require certification with versioning enabled.**
    Setup reads `GetBucketVersioning` (GCS's
    [`GET ?versioning` XML API](https://docs.cloud.google.com/storage/docs/xml-api/get-bucket-metadata#versioning)
    uses the same `Status` values). `Enabled` and `Suspended` require the selected
    profile's separate passing date and suite version; otherwise setup returns
    `StorageSetupError::S3VersioningUnsupported`. Detected and custom settings
    never inherit this certification, even when their settings equal a
    catalogued profile's. Coven addresses current objects; it does not choose
    older versions to repair an incorrect publication.
    A versioned Ceph RGW 20.2.4 bucket accepted concurrent conditional creates
    as separate versions: both writers received success while readers saw only
    one writer's bytes. Sequential setup probes passed that bucket. Those
    probes therefore cannot establish versioning support.
  - A refused or unimplemented versioning read leaves the state unknown.
    Profiles without versioning certification refuse it with
    `S3VersioningUnknown`, retaining the native cause; certified profiles may
    proceed and log the unknown state. Other failures, including malformed
    responses and transport failures, fail detection. A member's policy needs
    bucket-level `s3:GetBucketVersioning`, separately from its prefix-scoped
    object/list permissions; GCS needs
    [`storage.buckets.get`](https://docs.cloud.google.com/storage/docs/using-object-versioning#required_roles).
    Setup never
    changes the owner's versioning setting. The owner must keep the bucket in
    a supported state while it holds the store; every setup reads it again.
    Hosted R2 has no object versioning and
    [no versioning API](https://developers.cloudflare.com/r2/api/s3/api/): its documented
    absence, together with its refused read, is `Unavailable`, not an unknown
    owner setting. This exception requires R2's catalogued hostname, not an
    R2 name at an arbitrary endpoint.
  - **Every S3 setup checks safety against the server** before the provider
    check below, whatever selected its settings: detection runs, and the
    effective settings, detected, named or custom, must agree with what it
    observed. After the versioning check, setup refuses in order with `StorageSetupError::ProviderCheck`
    naming the first failed check:
    - `CreateOnce`: the single-PUT condition was not enforced, or changed the
      occupied object's bytes or time;
    - `MultipartCreateOnce`: the multipart publication method did not refuse
      an occupied name with its bytes and time unchanged, or, for conditional
      completion, an unfinished upload was visible;
    - `PublicationTime`: published multipart objects are not dated by
      complete publication (conditional completion with a start timestamp,
      or a copy not dated later than its source), or the recorded multipart
      timestamp contradicts the observed one;
    - `Behavior`: another setting contradicts an observation: a signer the
      service refused, a listing protocol that failed or disagreed, no
      checksum policy where one was returned, stated scope where scope was
      missing, checksum requests the service refused, generation revision or
      Google headers without GCS's headers, S3 headers where GCS's arrived,
      or a refused zero-entry listing at a missing object's key.
    A manual override contradicting the observations is refused this way and
    never silently accepted. Where no setting in the menu makes a check hold,
    no override can either: a server that accepts writes to an occupied
    name, e.g. Garage, is refused at setup with `CreateOnce`.
  - Certification suite version 1 passed GCS and R2 on **2026-10-13**, and
    suite version 2 passed RustFS and SeaweedFS the same day. Suite version 3
    detects with the production probes; the same day it reproduced each of
    those four entries and passed their setup safety check. Suite version 4
    passed Ceph RGW 20.2.4 with versioning disabled on **2026-10-14**, and
    RustFS 1.0.1 the same day with versioning both disabled and enabled. It
    failed SeaweedFS 4.48 in both states: concurrent conditional multipart
    completions both succeeded, so SeaweedFS's suite 2 certificate is
    withdrawn and it is **uncertified**. AWS is **uncertified**: there is no
    live account. The table's selected
    behaviors are:

    | Profile | Signing | Single PUT | Multipart publication | Native multipart time | Checksums | Listing | Revision |
    | --- | --- | --- | --- | --- | --- | --- | --- |
    | AWS (uncertified) | AWS SigV4 | `If-None-Match: *` | Copy with `If-None-Match: *` | Start | Scope stated | V2 | ETag |
    | GCS (certified) | Google HMAC | `x-goog-if-generation-match: 0` | Copy with the same generation condition | Completion | Scope stated | V2 | Generation |
    | R2 (certified) | AWS SigV4 | `If-None-Match: *` | Conditional completion | Completion | Unstated scope ignored | V2 | ETag |
    | RustFS (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Conditional completion | Completion | None returned | V2 | ETag |
    | SeaweedFS (uncertified) | AWS SigV4 | `If-None-Match: *` | Conditional completion | Completion | None returned | V2 | ETag |
    | Ceph RGW (certified, unversioned) | AWS SigV4 | `If-None-Match: *` | Conditional completion | Completion | None returned | V2 | ETag |

    RustFS alone supports versioned buckets: RustFS 1.0.1 passed every suite 4
    race with versioning enabled. GCS's supplied live key could neither read
    bucket versioning nor create a second bucket for certification.
    SeaweedFS 4.48 fails the multipart races with versioning enabled or not.
    AWS remains uncertified. R2 has no versioning feature. Ceph
    RGW 20.2.4 fails the versioned races. See
    [suite 4 evidence](s3-certification.md#concurrent-certification-suite-4)
    for the live runs and their bucket states.

    AWS and R2 request checksums; every profile but GCS consumes S3
    response headers. RustFS, SeaweedFS and Ceph RGW are not asked for checksums they
    never return. GCS supplies
    its hashes without a checksum-mode request; the adapter translates Google
    headers. The ETag revision behavior uses native versions or `coven-revision`
    metadata for replaceable objects, as specified below.
  - To certify a service, put its bucket, region, endpoint and HMAC keys in an
    env file using `COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`.
    Run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification -- --ignored --nocapture`.
    Select a self-hosted profile with `COVEN_LIVE_S3_PROFILE=CephRgw`
    (or `RustFs` / `SeaweedFs`); otherwise the suite detects its settings.
    The ignored test runs production detection from the location's settings,
    or the plain S3 settings where detection must supply them, and prints
    every observation. A detected entry that fails the setup safety
    check above prints as uncertified with each failed check. The candidate
    then runs the adapter's full live contracts, including setup, reads,
    ranges, resumption, replacement revisions and listing lag. Suite 4 records
    the bucket's versioning state and adds six concurrent checks: 2 and 8
    conditional PUTs, 2 and 8 multipart publication requests, PUT racing
    multipart publication, and 2 replacements conditioned on the current ETag
    or generation. Copy profiles assemble their temporaries before racing the
    publishing copy. Each trial uses one fresh destination and distinct 64-KiB
    payloads, with every writer released by one barrier. Each request's start,
    end and outcome are recorded; all intervals must overlap. Exactly one
    request must succeed, every other request must refuse the condition
    (`AlreadyExists` or `Conflict`), and the stored bytes must be the winner's.
    Two-writer checks run 200 trials each; eight-writer checks run 50 each.
    A non-overlapping trial fails certification too. Replacement races are
    required, not observations exempted from failure. A service that
    returns no checksum on any probe, even with checksum mode, is recorded as
    returning none: §4 permits a missing checksum, so suite version 2 accepts
    that observation where version 1 failed detection. Each §4 requirement
    prints PASS or FAIL, and cleanup removes the run's objects, temporaries
    and uploads even after a failed assertion, including object versions and
    delete markers in a versioned run. Certification needs permission to list
    and delete those versions; ordinary member object deletion still addresses
    the current object. The suite prints a Rust profile
    entry with the passing date and suite version; commit it only after every
    check passes, alongside
    the [raw certification evidence](s3-certification.md). Never publish the
    env file or credentials. A live sample measures lag; it does not prove M.
    Only a passing suite with the recorded state `Enabled` supplies versioning
    certification. A passing disabled-bucket run never supplies it. Certification
    exercises a versioned candidate even when setup refuses it, reporting both
    verdicts separately; an unknown bucket state cannot be certified.
  - AWS S3 creates once with `If-None-Match: *` on `PutObject`; an occupied
    destination returns HTTP 412 `PreconditionFailed`, mapped to
    `AlreadyExists`. Conditional replacements use `If-Match` with the observed
    ETag. Multipart parts remain unpublished until completion. AWS enforces
    `If-None-Match: *` on `CompleteMultipartUpload` too, but its multipart
    `Last-Modified` records initiation, so the AWS profile publishes multipart
    uploads by copy with `If-None-Match: *` on `CopyObject`, which
    [AWS documents](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
    since October 2025. That profile is uncertified; the setup safety check
    verifies it against the account's bucket. Listing uses `ListObjectsV2`, an
    exact prefix and every continuation token; immediate device discovery also
    uses delimiter `/`.
  - GCS ignores `If-None-Match: *` on PUT. Create-once PUT instead sends
    `x-goog-if-generation-match: 0`. An occupied destination returns HTTP 412
    `PreconditionFailed`, mapped to `AlreadyExists`, without changing its
    bytes or publication time. Conditional replacements use the observed
    generation in `x-goog-if-generation-match`; each replacement's revision
    is its new `x-goog-generation`, including equal-byte replacements.
    Unconditional replacement remains available for a device's own positions.
    Requests use Google V4 HMAC authentication and `x-goog-*` headers: GCS
    refuses requests mixing Google and AWS extension headers.
  - GCS XML multipart initiation rejects generation conditions and completion
    does not enforce create-once, even though its measured native timestamp is
    completion. Coven therefore publishes by copy. Every copy-publication
    profile uploads unconditionally to
    `<prefix>/<store>/.coven-uploads/<session>`, where `<session>` is the
    upload's coven-generated UUID, recorded alongside the native multipart id
    before sending any parts. This reserved namespace is distinct from every
    coven object path in D10. Retrying or resuming that recording derives the
    same name, including after multipart completion or a lost copy reply.
  - Copy publication sends the assembled temporary with one server-side
    `CopyObject` to the destination, carrying the profile's destination
    condition (`x-goog-if-generation-match: 0` for GCS). The
    destination's publication time is the copy's completion, never temporary
    creation or multipart initiation. An occupied destination stays untouched;
    the session marker distinguishes this session's lost successful reply
    from another object's occupation. Successful and definitively refused copies
    delete the temporary before reporting completion or the refusal.
    Network failures and throttling retain it for the same session.
    A failed deletion fails the call and retains the recording for retry;
    an operation error and deletion error are both preserved.
  - Aborting a copy-publication session aborts its native multipart upload
    and deletes its temporary, including when the native upload has already completed.
    A crashed device's recorded session therefore remains usable by the same
    abandonment cleanup. Temporaries remain discoverable in the reserved
    namespace but are excluded from published-object listings. GCS uses
    `ListObjectsV2` with the requested prefix, continuation tokens and delimiter
    as AWS does; published objects retain the server's `LastModified`.
  - AWS `ConditionalRequestConflict`, GCS `OperationAborted` and Ceph RGW
    `ConcurrentModification` indicate a
    concurrent mutation, never throttling.
    A single-request PUT can retry its fixed bytes. At conditional multipart
    completion or copy publication, it invalidates the recorded session
    instead: discard its part list, abort the native upload and delete any
    copy temporary.
    Preserve both causes if abort or deletion also fails. Record the invalidated
    session even on failure; it retains only the identifiers needed for cleanup
    and cannot complete or copy again. A replacement session starts only after
    cleanup succeeds, at offset zero with the same path, size and fixed bytes.
    An occupied destination still requires complete-byte comparison (§10).
  - Every S3 location, catalogued, named, custom or detected, runs the setup
    safety check above and every provider check below, including
    `CreateOnce`. Accepting a second create is a protocol failure, even if the
    service claims compatibility; certification does not exempt a connection
    from either check.
  - R2's live certification selected conditional completion without a copy:
    both single PUT and multipart completion refused occupied names with HTTP
    412 and preserved their bytes and times. Unfinished uploads were absent
    from HEAD, GET and listing; multipart `Last-Modified` fell between the
    completion markers. R2 returned a checksum without its scope, so the
    adapter discards that checksum before metadata parsing and SDK body
    validation. A stated scope remains usable; none is inferred from the
    algorithm or ETag. Cloudflare's [S3 compatibility table](https://developers.cloudflare.com/r2/api/s3/api/)
    documents operation and checksum differences; its [copy extensions](https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-copyobject-for-the-destination-object)
    document `cf-copy-destination-if-none-match`. The R2 profile does not use
    that extension because native conditional completion passed. Profile values
    come from the live suite, rather than a compatibility claim.
  - RustFS, SeaweedFS and unversioned Ceph RGW refused occupied names with HTTP 412 on
    PUT and multipart completion, preserving bytes and times, and their
    multipart `Last-Modified` fell between the completion markers, so they use
    conditional completion without a copy. That sequential refusal does not
    hold under concurrency on SeaweedFS 4.48: two racing conditional
    completions of one name both succeed. None returned a checksum, even
    with checksum mode; their profiles do not request one and rely on the
    complete-byte comparison required for a missing checksum (below).
  - `RequestTimeTooSkewed` means the device clock is off, not that access
    was denied. Retry the request once with the server offset the SDK
    learned from the response.
  - If that retry still fails for clock skew, report `ClockSkew` with the
    provider's cause ([E5](api.md#e5-storage-and-sync)).
  - E.g. Ben's laptop signs with yesterday's date. The first reply supplies
    the offset; its one retry uses it without asking Ben to change his keys.

- OneDrive upload sessions expose unfinished items, so coven uploads to
  `<store>/.coven-uploads/<session>` inside the configured folder. The reserved
  namespace is distinct from every coven object path in D10. The session's
  coven-generated UUID is recorded with its native upload capability before
  sending parts; retries and resume derive the same temporary name.
  - After all parts are stored, coven publishes with one server-side Graph
    `copy` to the final parent and name, with
    `@microsoft.graph.conflictBehavior=fail`. It follows the returned monitor
    URL until the asynchronous copy completes. An occupied name keeps its
    original bytes and time. Recovery after a lost publication reply requires
    complete-byte verification before confirming an existing object.
  - The copy retains the temporary's `createdDateTime`, which records session
    start. Publication therefore uses the top-level `lastModifiedDateTime`,
    assigned by the copy. Coven never modifies a published immutable object;
    reads and listings retain that first publication time.
  - Successful and definitively refused copies delete their temporary.
    Transport failures and retryable responses retain it for the recorded
    session. A failed deletion fails the call; operation and cleanup failures
    are both preserved, and the recording remains available for retry.
  - Abort cancels an open native session and deletes its temporary, including
    after native upload completion. The same recorded-session abandonment
    cleanup removes leftovers after a crash. Published-object listings exclude
    the reserved temporary namespace, including unfinished uploads.

- What coven needs from a provider:
  - create an object, refusing an existing path without replacing its bytes,
    in one request or, past the provider's single request limit, through its
    resumable upload;
  - read it, whole and by range; complete-object reads stream with bounded
    buffers so the reader checks chunks without a request per chunk
    ([One sync pass](sync-pass.md#one-stream-checked-as-it-arrives));
  - list a prefix, with when storage stored each object; each query is scoped
    to that prefix, across all its pages, without a change feed;
  - list immediate device folders under `store-log/`, without enumerating
    their contents. This discovers unknown writers, not pages of history;
  - get one object's status by its exact path: absent, or its complete
    encrypted size, first publication or replacement time, object id and
    revision. A revision changes when replacement changes the encrypted bytes,
    even when size and timestamp match. Identical-byte writes may retain a
    revision (Dropbox does); they do not change a reader's retained bytes.
    An error is never absence;
  - delete;
  - grant and revoke a member's access, where the provider can: Google
    Drive, Dropbox, OneDrive and iCloud share with an account.
- On Drive, Dropbox and OneDrive, every directory component in D10 is a
  real provider folder. List that folder directly, never the configured
  root followed by client-side filtering. S3 uses the exact key prefix;
  CloudKit queries the exact indexed ancestor directory in the selected zone.
  Folder ids and known path-to-object ids stay with the connection. An
  unknown Drive object uses an exact parent/name query; its id is then cached.
  Failed discovery never installs a partial catalog as complete.
  - On S3, device discovery lists `store-log/` with delimiter `/`. Drive,
    Dropbox and OneDrive list its immediate children, without recursion.
    CloudKit queries the indexed store-log directory and number for entry 1
    of each writer; those permanent records represent its device folders.
    No adapter downloads the history to derive the set of writers locally.
  - S3 and Dropbox downloads include their metadata without a preliminary
    status request. Drive and OneDrive fetch exact metadata before downloading.
    Their replaceable objects also require a metadata recheck after downloading
    to reject a concurrent replacement. On Drive, an
    unknown file id requires one [parent/name query](https://developers.google.com/workspace/drive/api/guides/search-files).
    An empty complete result is the miss; a hit adds exact metadata and one
    [file download](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get).
    Count metadata requests, query pages and redirects in `M` unless already
    counted as a discovery miss; an incomplete search
    or provider error is not absence. Cache the id for subsequent reads.
  - Ana's `files/` folder can hold 20,000 photos. Reading `store-log/`
    never enumerates those photos or another store's objects.
- Listings may lag publication. Every provider has the same listing margin
  `M = one hour`. At provider time L, a completed listing must include every
  still-existing object in its scope stored strictly before `L - M`.
  Objects exactly one hour old or newer may be absent. The store-log device
  listing must include each writer whose permanent entry 1 meets that age.
  Exact-path reads and status must reflect already completed publication;
  recent absence from a listing is not evidence of absence at an exact path.
  - Violating this requirement can make a later entry wrongly final:
    receiving the hidden older entry can reverse its replay result
    ([checked example](proofs/io/CovenIO/ListingMarginExamples.lean)).
  - Live provider tests measure listing lag separately from exact reads;
    the worst observed lag is 5.5 seconds. Measurements do not prove the
    one-hour bound; it remains a provider requirement.
- Status uses these calls and provider timestamp granularities. The time
  unit `δ` for §9 comes from the returned timestamp's representation:
  - S3 HEAD: `Last-Modified`, in whole seconds, so `δ = 1 second`
    ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)).
    S3 listings use that same precision: truncate fractional XML listing times
    to whole seconds. GCS listings include milliseconds while HEAD and GET
    omit them; keeping both representations would give one immutable publication
    different times depending on the observation call.
  - Drive file metadata, with an exact name lookup when its id is unknown:
    `createdTime` for creation and `modifiedTime` for replacement, retaining
    their RFC 3339 fractional-second precision
    ([files](https://developers.google.com/workspace/drive/api/reference/rest/v3/files)).
  - Dropbox metadata: `server_modified`, in whole seconds, so
    `δ = 1 second`; never use `client_modified`
    ([timestamp format](https://github.com/dropbox/dropbox-api-spec/blob/main/common.stone)).
  - OneDrive metadata: the top-level `lastModifiedDateTime` for publication
    and replacement, retaining the returned
    `DateTimeOffset` precision; never use the client's `fileSystemInfo` times
    ([driveItem](https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0)).
  - CloudKit record fetch: the server's creation or modification date,
    with `δ = 1 millisecond`, the unit of CloudKit's date/time representation
    ([types and dictionaries](https://developer.apple.com/library/archive/documentation/DataManagement/Conceptual/CloudKitWebServicesReference/Types.html)).

  For Drive and OneDrive, `p` returned fractional-second digits give
  `δ = 10^(-p) seconds`: no fraction means one second, three digits one
  millisecond. Preserve the returned precision in comparisons; do not round
  a fractional timestamp to seconds or invent an order within an equal time.
  The listing margin M is independent of that precision and is at least δ.

  Exact status and reads carry the complete encrypted size, native object id,
  revision, timestamp and its resolution, plus any returned checksum. A ranged
  read still describes the complete object. Drive uses `headRevisionId` (its file `version` can also change on reads), OneDrive
  uses `eTag`, and Dropbox uses `rev`. CloudKit uses the record's `recordChangeTag`. On replaceable S3 paths, GCS uses its generation; AWS and R2 use a
  version id when versioning is enabled, otherwise a fresh `coven-revision`
  value stored atomically in the replacement's user metadata. Immutable S3
  objects use their ETag. S3 listings fetch exact status for replaceable paths
  because `ListObjectsV2` contains neither versions nor user metadata.

  Checksums name their algorithm, encoding and scope: whole-object or
  multipart composite. An ETag or revision is never inferred to be a checksum.
  Drive exposes MD5, Dropbox its block-based content hash, OneDrive SHA-1 or
  QuickXorHash, and S3 its explicitly returned checksum and scope. GCS's
  CRC32C is a whole-object checksum. R2's unscoped checksum is absent from
  coven's metadata; its scope is never guessed. RustFS, SeaweedFS and Ceph RGW return
  none; a checksum returned under their profile, scoped or not, contradicts
  the certified observation and fails as a protocol error rather than being
  used. A missing checksum requires the byte
  comparison; a composite cannot confirm a whole-object digest. Metadata and
  downloaded bytes must describe the same revision: Drive and OneDrive
  recheck replaceable metadata after download and fail if it changed.

  File status and upload confirmation use this call, not a folder listing.
  Count any name lookup and missing-container check separately. S3 HEAD's
  bodyless 404 requires a bucket check: every S3 service uses a zero-entry
  listing with the exact object's key as its prefix, under the configured
  prefix and store id. Catalogued profiles use ListObjectsV2; a custom V1
  profile uses ListObjects. Where the service allows it, a member key is
  scoped to the store's prefix; HEAD Bucket requires bucket-list permission
  without a prefix condition, which that key does not grant. Object absence
  comes from the exact HEAD, never from the listing. A miss remains two requests: one HEAD and one listing.
  A revision proves freshness, not byte
  equality: occupied immutable paths still require §10's complete comparison.
  Snapshot confirmation still checks the complete-object checksum (§15).
  - Ben replaces his positions twice within one storage clock tick with
    equal-length bytes. Ana notices the new revision and reads the new post.
- Storage times come from the provider, on one clock for this store.
  An immutable object's time is its first complete publication; retrying
  an occupied path does not change it. Replacing posted positions or a clock object gets
  the replacement's storage time. Publication times do not go backwards.
  - Storage time advances no faster than real time, apart from at most one
    day of forward drift over any interval of up to 30 real-time days.
    With §15's sampling and sleep-inclusive monotonic elapsed time, current
    storage time is therefore at most `T + monotonic elapsed + one day`
    while the recent-return check holds. This is an environmental
    assumption, alongside §10's request-duration bound and §13's storage
    cut-off assumption; nondecreasing publication times alone do not imply it.
- Native publication gaps remain visible in the profiles and certification.
  GCS conditional-copy timestamps and R2 single-PUT and multipart timestamps
  passed the completion checks, as did RustFS, SeaweedFS and unversioned Ceph RGW single-PUT and
  multipart timestamps.
  AWS single-request PUT timestamps describe
  completion; AWS multipart `Last-Modified` instead records initiation
  ([system metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)).
  That native AWS multipart path does not satisfy the complete-publication
  requirement above, so the AWS profile publishes multipart uploads by
  conditional copy instead; the adapter never invents a later time. A
  start-time service needs a conditional copy that dates complete
  publication, and the setup safety check refuses any S3 location whose
  multipart objects are not dated that way. OneDrive
  publishes session uploads by conditional copy as described
  above. CloudKit's atomic publication and asset-session guarantees require live
  verification through a signed, entitled host.
- Device paths have one intended writer: the device they name. Retrying
  that writer uses fixed bytes. A copied identity can violate this;
  the checks in §10 detect it before sending or when a path is occupied.
  - Sealed key copies have one writer per path too. Several devices can
    supply the same key to the same member, but each uses its own
    `<store>/keys/<writer>/` folder in D10. Other writers never occupy that path.
    Duplicate valid copies have the same key meaning, not the same bytes.
  - On Google Drive, which allows two files with one name, a retry first
    looks for its own earlier copy.
- On the providers that share with an account, only the member whose
  account holds the store can share it and take it back, so:
  - that member's devices make invites ([§12.2](#122-adding-a-person));
  - any admin removes a member, and that member's access is taken back by
    the owner's device when it applies the removal
    ([§13](#13-removing-members-and-devices));
  - coven promises the access intended by the current replay. Provider
    requests take time, can fail, and need not match that intention at
    every instant;
  - each owner's device runs its grant and revoke requests one at a time.
    Before the next request it reads the current intended access. If a
    completed request has become obsolete, it records and performs the
    opposite request before reporting that account's work complete;
  - pending and failed requests remain in the existing operation journal
    and the pending list (§18, §19.1). Security work starts immediately;
    it does not wait for entry finality.
  - Sharing is per account, not per invite: taking access back takes the
    account's access, whatever shared it.
  - So the owner's device leaves an account shared while a member in its
    store log reaches storage through it, or another of its own open
    invites names it.
  - An invite made at the same time on another of the owner's devices can
    still lose its access this way: the invited device's join fails with
    the provider's permission error, and an admin invites them again, as
    with access taken back in [§13](#13-removing-members-and-devices).
  - Taking access back removes only what grants that one account; a share
    that also grants others is left, and reported to the owner, who
    changes it in the provider.
- Every storage setup, including reconnecting an existing device, checks the
  candidate provider with the supplied credentials before reserving a store-log
  entry or committing credentials, keys, location or restore code. It creates a
  fresh sealed test object, requires a second create to be refused, verifies
  whole and ranged reads, and requires its scoped listing query to succeed.
  If that query returns the fresh probe, its size must agree; absence inside
  `M` is permitted and establishes no completeness claim. The check then deletes
  the object and verifies absence by exact-id read. On S3 the safety check
  above runs first. A failure names the failed check and preserves the
  previous connection ([E5](api.md#e5-storage-and-sync)).
- Every storage path starts with the store's id, including the provider
  check's temporary objects and S3 detection's reserved
  `<store>/.coven-check/` probes ([D10](format.md#d10-paths)).
  - Several stores can share one folder or zone, and one bucket prefix
    where member keys can be limited to a prefix. On an S3 service whose
    keys reach a whole bucket, each store has its own bucket (above).
  - Setup writes only this store's prefix and reads only it, apart from
    the bucket-root listing that creates a store needing its own bucket
    (above); other stores and unrelated objects outside it do not compete
    with this store. S3's read-only bucket-versioning query has bucket scope
    because the owner setting applies to every object; it reads no other
    store's data.
  - E.g. Ana creates Household and Ben creates Garden in the same folder.
    Their store ids differ, so their first entries have different paths.
- Reconnecting uses the recorded provider location. Setup does not move an
  existing store to another location: its other devices still use the
  location in their restore codes.
- Access-key creation and deletion are specific to each S3 service.
  Coven may make and delete member keys using the storage account's admin
  access, held in memory for one operation and never stored. Admin credentials
  never enter the store log, invites, logs or device key custody; member
  credentials remain separate and follow their ordinary custody rules.
  - The person chooses a signed-in CLI and its profile/account, browser
    sign-in, a pasted admin key, or guided console steps. Coven reads no CLI
    configuration and runs no CLI credential helper before the person chooses
    that CLI. Provider-admin access does not itself confer a store admin role.
  - With guided steps, the admin creates or deletes the key in the provider's
    console. With direct provider calls, coven performs the operation using
    the supplied admin access. Both paths record member access and pending
    deletion work under §13; a retry needing admin access obtains it again.
  - The sources (E9), on desktops: the AWS CLI's
    `aws configure export-credentials --format process` for the chosen
    profile, short-lived keys with their session token; `gcloud auth
    print-access-token` for the chosen account; Google sign-in through the
    app's Google client with the installed-app loopback, PKCE and the
    `cloud-platform` scope, requesting no refresh token; AWS IAM Identity
    Center's device authorization (register a public client, show the
    verification page and code, poll at the given interval, then fetch the
    chosen account and role's credentials). On every platform, the person may
    paste an admin access key or API token. A CLI runs only after the person
    chooses it, with no input; its output is parsed in memory and erased, and
    a failure reports only its exit status, since CLI text can echo
    configuration. Guided steps show the bucket, the store's prefix and the
    member policy below, filled in.
  - Each catalogued profile's const entry names its key minter, function
    pointers that make, roll back, delete and read a member key with admin
    access. AWS uses IAM at its global endpoint, signing for `us-east-1`: a
    user under the path `/coven/`, tagged `coven-store` and `coven-member`
    with the store id and the member or invite id (never an email), the
    member policy below as the inline policy `coven-member`, and one access
    key. SeaweedFS's embedded IAM, on the location's S3 endpoint and region
    (the operator sets `-iam.readOnly=false`), takes the same policy through
    `CreatePolicy` and `AttachUserPolicy`: its `PutUserPolicy` drops
    conditions and refuses `s3:ListMultipartUploadParts`. Ceph RGW accounts
    (Squid or newer) take the inline form at the location's RGW endpoint and
    region, using an account-root key or a user's delegated IAM access
    ([RGW IAM](https://docs.ceph.com/en/squid/radosgw/iam/)).
    GCS, R2 and RustFS have their own minters; a custom profile uses
    guided console steps. On **2026-10-14** the RustFS, SeaweedFS and Ceph
    RGW 20.2.4 minters each made a key live with an admin key, passed the
    member-side check below, were refused writes, reads and listings outside
    the store's prefix, revoked the key twice, read it absent and saw the
    service refuse it ([evidence](s3-certification.md#member-keys)). Run
    `COVEN_LIVE_S3_ENV=<file holding the admin key> COVEN_LIVE_S3_PROFILE=<name>
    cargo test -p coven-storage s3_member_key_lifecycle -- --ignored --nocapture`.
  - On GCS each member key is the HMAC key of its own service account. The
    minter reads the bucket's project through the JSON API and refuses a
    bucket without uniform bucket-level access, which managed folders need.
    It creates the service account `<name>@<project>.iam.gserviceaccount.com`,
    whose description names the store and member, creates the store's managed
    folder `<prefix>/<store>/` if absent, and binds the account to it as
    `roles/storage.objectUser`, retrying once when the folder's policy changes
    concurrently. HMAC keys an earlier attempt left are set inactive and
    deleted; then `hmacKeys?serviceAccountEmail=` creates the key, whose
    access id is the member key id. The admin access is an OAuth access token
    with the `cloud-platform` scope. A new HMAC key can take up to 60 seconds
    to work, so the member-side check retries for 60 seconds.
  - Revoking a GCS key sets it inactive and deletes it, deletes the account's
    other HMAC keys, removes its folder binding and deletes the service
    account. The managed folder is the store's and stays. Absence is read
    from the account's HMAC key list, which a deleted account leaves empty.
  - On R2 each member key is a Cloudflare account API token, named after the
    attempt and limited to the store's own bucket
    (`com.cloudflare.edge.r2.bucket.<account>_<jurisdiction>_<bucket>`) with
    the `Workers R2 Storage Bucket Item Write` permission group, object reads
    and writes. The location's endpoint host names the account and its
    jurisdiction. The token id is the S3 key id and the hex SHA-256 of the
    token value its secret. Tokens an earlier attempt named are deleted
    first. The admin access is an API token allowed to write account tokens,
    which can grant anything its owner holds. Revocation deletes the token;
    absence is read from the token itself. Because the key reaches its whole
    bucket, its member-side check requires the account's bucket listing to be
    refused.
  - On RustFS each member key is a service account made through
    `/rustfs/admin/v3/add-service-account` with the member policy inline, as
    plain JSON; MinIO's `/minio/admin` alias expects madmin's encrypted
    bodies. Every member key's parent is one dedicated user, `coven`, created
    once with a random secret nobody keeps and granted `readwrite`; a service
    account's access is its parent's intersected with its own policy.
    Deleting that parent would delete every member's key, so coven never
    deletes it. Accounts an earlier attempt named are deleted first.
    Requests are signed with Signature Version 4 for `s3`. Revocation calls
    `delete-service-account`; absence is read with `info-service-account`.
    RustFS answers a delete of an absent account with HTTP 400
    `InvalidRequest`, a status other refusals share, so a refused delete
    succeeds only when `info-service-account` then reads the account absent.
  - The member policy allows `s3:GetObject`, `s3:PutObject`,
    `s3:DeleteObject`, `s3:AbortMultipartUpload` and
    `s3:ListMultipartUploadParts` on `arn:aws:s3:::<bucket>/<prefix>/<store>/*`,
    and `s3:ListBucket` on the bucket under the condition `StringLike`
    `s3:prefix` `<prefix>/<store>/*`. A separate, unconditioned statement
    allows `s3:GetBucketVersioning` on the bucket, so the member's setup can
    read the owner's versioning setting without granting bucket-wide listing
    or writes outside its store. It never adds a `Deny` on
    `StringNotLike` `s3:prefix`: a request carrying no prefix, such as a bucket
    check, matches the negated condition and would be denied.
  - Making a key is one journaled operation (§18). Before the first provider
    call the journal records a fresh attempt and the principal's name derived
    from it, `coven-` and 24 hex digits, so every retry finds what an earlier
    attempt made: an existing user or policy is kept, and keys whose secret no
    attempt holds are deleted before a new one is created (an IAM user holds
    at most two). The minted key's secret then joins the journal, as an
    invite's key does.
  - Before delivery the key is checked from the member's side. Under it, an
    object in `<store>/.coven-check/<run>/` is created, refused a second time,
    read back and deleted, and that prefix is listed. Where keys reach a
    prefix, a listing of the location's base prefix, outside every store,
    must be refused; where they reach a whole bucket, the account's bucket
    listing must be. Otherwise the check fails with
    `StorageFailure::KeyBeyondStore`. Authentication and
    permission failures are retried each second within the service's
    retry window: 60 seconds for AWS IAM and Ceph RGW, and 30 for SeaweedFS,
    whose gateways read identity changes asynchronously. The 60-second window
    is a retry limit, not a verified provider propagation guarantee; a key
    still refused at the limit fails the operation.
  - A failed mint or check rolls back what the attempt's name created — keys,
    policy and user — and reports the failure, with the rollback's own failure
    if that fails too. Admin access is held only by the operation that needs
    it and released once its key is checked or its step fails; after a restart
    the step waits with `PendingReason::AdminAccessRequired` until a retry
    supplies admin access again.
- Posted positions live at `<store>/positions/<device>` ([§6](#6-syncing-writes)).
- Sealed store and circle keys share `<store>/keys/<writer>/<n>`; their
  clear prefixes name audience, key and recipient ([§11](#11-keys), D9).

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- A *row change* is one row's insert, update or delete within a write.
- SQLite's session extension records the row changes each write makes.
- Each write commits, together:
  - its rows, as they now stand;
  - its *write record*, waiting to be uploaded in coven's `_coven_uploads`
    table:
    - its row changes: which rows, which columns, old and new values;
    - the original creator identities, direct targets and foreign-key causes
      needed to check owner permissions (§8.8), including actions coalesced
      into one row change;
    - which device wrote it, its number, its timestamp, and what it had
      read;
    - the schema version it was made with;
  - so every committed write gets uploaded, even after a crash.
- A write needs no key: the record waits unencrypted and unsigned, and its
  upload encrypts and signs it ([§6](#6-syncing-writes)).
  - So local writes need no store key, including before storage or identity
    has been initialized ([E1](api.md#e1-opening)).
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    had read: ben-phone 8, carol-tablet 1
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   "errands"  delete
  ```

- Database callbacks release their connections before propagating a panic.
  An uncommitted transaction rolls back, and later calls can reuse the writer
  and readers.
- If rolling back a transaction fails, its connection may still be inside
  it: the writer would treat its uncommitted changes as committed in its own
  later reads and writes, and a reader would keep reading its old snapshot.
  Every later call and live query on that handle, read-only or not, fails
  with `RollbackFailed` until the store is reopened.
  - A failed write or check reports both failures.
  - A read ends its snapshot by rolling back. When only that fails, the read
    reports it; a read that failed itself keeps its own cause.
  - A panicking callback's panic still propagates, and the failed rollback is
    logged.
- Reads run on several read-only connections at once.
- Another process, such as a widget, can open the store for reading only
  while the app has it open.
- The app can subscribe to a query; it reruns only when rows it read change.
- Coven keeps its own internal tables in the same database. The app can't
  read or write them.
- In a table whose rowid isn't its primary key, the app can read the rowid
  but not change it once the row exists: it is SQLite's private address for
  the row, which coven doesn't sync or watch, and which VACUUM can renumber.

## 6. Syncing writes

A sync pass has three phases: catch up on membership and key copies; make
keys and provider access match that view; then sync data. The request order
and pending-work gates are in [One sync pass](sync-pass.md).
File uploads run in their own worker (§16.5); a pass does not wait for them.

- Each device uploads its writes to its own log in storage, and the other
  devices download them.
- No two devices write the same object, so devices never have to coordinate
  their uploads.
  - Each object is one write from that device: its write record ([§5](#5-local-database)),
    encrypted.
  - A write is one object however big, encrypted in chunks like a file
    ([§16.2](#162-storage-and-naming)), so the format puts no limit on
    its size.
  - A device downloads and checks it a chunk at a time, and applies it in
    one transaction once every chunk and the signature check out.
  - Its plaintext record waits in `_coven_uploads` as one value. The record,
    sealing key ids and row overhead must fit SQLite's 1 GB value limit;
    a write exceeding it fails at commit with `DbError::TooLarge`.
  - E.g. Ana imports 50,000 notes in one transaction: one write, in many
    chunks.
  - It is named `<store>/devices/<device>/<n>`, created once and never changed.
  - `<n>` counts that device's own writes: 1, 2, 3, with no gaps.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.
  - A retry finding an occupied path reads and compares the complete bytes.
    Equal bytes count as stored; different bytes reset this stale device
    (§10). A failed comparison read leaves the upload pending.
- A device uploads its writes in number order; a write never goes up
  before an earlier one.
- The first attempt records the header's and each part's sealing key ids
  in `_coven_uploads`, in the transaction marking the attempt. For an audience
  the member still belongs to, it chooses the newest usable key under §11,
  excluding keys known to have reached an excluded member or a retired or
  noncurrent sealing credential
  ([queue proof: `first_key_excludes_known_recipient`](proofs/queue.md#what-is-proved),
  [credential exposure proof](proofs/storelog-data.md#member-key-replacement)).
  A removed circle member's pre-removal parts may instead use a key they
  already received (§14.6;
  [checked history: `departed_member_counterexample`](proofs/queue.md#checked-histories-and-limits)).
  Every attempt re-seals the plaintext with those keys and signs with the
  signing key current for the device's stable owner in the write's recorded
  `store_log_read`. That key is fixed for retries too; §10 refuses sending
  it once the current catch-up replaces it. Each chunk's nonce is derived from its encryption
  key, path, cleartext prefix, section, index and the SHA-256 of that
  chunk's plaintext ([D11](format.md#d11-keys-contexts-and-fingerprints));
  Ed25519 signatures are deterministic, so retries produce identical bytes.
  The queue keeps no ciphertext.
  - The plaintext, format and key choices never change after the first
    attempt, including after a crash
    ([queue proof: `Run.attempt_persists`](proofs/queue.md#what-is-proved)).
    Migration also preserves every untried upload's original schema,
    disposition and plaintext; conversion belongs to received interpretation
    (§17.1; [`conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
  - E.g. Ana encrypts write 6, then restores a backup that forgot the
    attempt. A different edit at write 6 has different nonce inputs even
    if her custody id survives. The same holds for two live copies.
    Identical retries still reproduce the original object.
  - E.g. Ana's phone commits a Gifts pin offline, then reads her removal
    from Gifts before uploading it: the pin's part is sealed with the
    Gifts key she held, and counts like any write made before she read
    her removal ([§14.6](#146-leaving-a-circle)).
- Store log entries likewise keep only their plaintext and sealing key id,
  fixed for that recorded entry before the first attempt, and re-seal
  identically on every retry. An operation's replay restart reserves a new
  entry; it does not rewrite the old entry or copies (§18;
  [checked history: `restart_changes_key`](proofs/queue.md#checked-histories-and-limits)).
  Sealed keys keep their fixed bytes because sealing uses a fresh ephemeral
  key pair; snapshots keep theirs because the database they describe changes
  ([§18](#18-operations)).
- A write record leaves `_coven_uploads` ([§5](#5-local-database)) once its upload succeeds.
- Each device remembers how far it has applied every device's log, in
  coven's `_coven_positions` table: one row per device, naming its last
  applied write's number.
- It posts positions to storage at `<store>/positions/<device>`, replacing
  its object when positions or pending records change (D8).
  - In its own write log it posts only writes already uploaded.
  - The posted positions form a causally closed applied past: every cause
    of every included write is included, and no unfinished reload is passed.
  - Pending uploads can prevent advancing that past. Pending records still
    travel: reuse the last publishable positions and omit fingerprints
    unless they describe exactly those positions.
  - Store-log positions count consumed entries, kept or dropped. They do
    not establish finality; that uses storage times and recorded reads (§9).
  - Check its signature with the device owner's signing key current in the
    object's signed `store_log` view. Unverified view fields are lookup hints
    only. An unknown device waits for registration; a wrong or missing
    signature is refused (§19.1). Historical positions remain usable under
    D9's explicit policy after key replacement; they grant no current authority.
- A missing prerequisite or failed read gets a pending record. A complete
  immutable write or entry that fails a permanent check stops its log at
  that object. Independent logs continue (§19.1).
- Every log has one writer per folder and gap-free publication numbers:
  writes, store-log entries and sealed key copies. Each starts at 1 and
  publishes in order. Discovery GETs the next number after the contiguous
  input already retained or covered by an applied snapshot.
  - A hit supplies the object itself. Fetch ahead in parallel up to the
    transfer limit, and stop scheduling beyond the first miss. Retain
    completed reads and their check results; never advance over a failed
    read or input not durably retained, or over a miss except for §15's
    certified deleted-number handling
    ([`committed_number`](proofs/io/CovenIO/CommonReload.lean)). Requests
    already in flight still count. Causal application keeps its own
    positions and waits, independently of this download position.
  - A miss means nothing new at that observation; only a completed retirement
    drain below makes it the last discovery probe.
    A missing number required by known causal or snapshot evidence uses
    §15's fresh-catalog rule; without certified deletion it remains a
    pending prerequisite
    ([`scan_spec`](proofs/io/CovenIO/FixedReload.lean)). Failures are never
    misses (§19.1).
  - Store-log entries and key copies are never deleted, so their terminal
    miss is exact. Write misses use the return-from-absence rule in §15.
    Snapshots and files are not discovered by next number.
- Discover unknown writers by listing only the immediate device folders in
  `<store>/store-log/`. Keep that set together with every device named by
  received registrations, including removed and replaced devices. A newly
  discovered writer starts at entry 1 and key copy 1; its writes start at
  the device's applied snapshot coverage, or 1 without coverage.
  Read writers learned from arriving registrations in this observation
  too, before treating its membership and copy evidence as complete.
  Registration precedes ordinary writes and independent historical sharing;
  creation's prerequisite copies are also reachable through its writer.
- Poll removed and replaced devices like active ones until a kept entry
  retiring them has been final for more than one day and five minutes (§9,
  §10). Finality itself requires a provider sample more than 30 storage days
  plus one hour plus δ after that entry, and can take longer while late
  entries arrive.

  Retain the qualifying storage time `F` that established this retirement's
  finality in the local catalog, with its retiring entry and device id.
  Save it atomically with the finality observation and keep it on reopening;
  do not refresh it on later passes. Using a later qualifying time initially
  is safe but delays the drain. Never infer `F` from the retirement's
  publication time. Wait for an observed storage time `T`
  strictly greater than `F + one day + five minutes` before the drain reads.

  Then read each of that device's three logs forward to a miss once, using
  §15's snapshot coverage before accepting write misses. Reuse this pass's
  reads only if they followed that qualifying `T`.
  Retain the complete inputs and settle in-flight reads before recording
  the device as drained. A failure, required gap or incomplete read leaves
  the drain pending; retry with retained progress under the shared backoff
  and §19.1's refusal rules. It is never a completed drain.

  `_coven_drained_devices` is local, keyed by device id, and records the
  kept retiring entry that justified each completed drain. Set it only in
  the successful transaction retaining all three completed observations,
  checking that the entry is still kept and the wait above has elapsed.
  It survives reopening, but is neither posted nor included in snapshots.
  Later passes skip that device's entries, writes and key-copy probes;
  keep its received history.
  Cached writes waiting for keys or application remain pending and are
  processed from retained inputs; draining does not claim they are applied.

  Replay derives retirement from kept entries. In the same replay transaction,
  remove any drain record and timing evidence whose retiring entry is no
  longer kept; if the device is active again, poll it again. A non-final
  removal can drop before any drain is allowed; a correctly established
  final entry cannot drop (§9).
  Remembered retirement alone never suppresses discovery.

  The last drain is exact for writes because §10 rejects publication after
  the kept retirement's 30-day deadline. Entries already have §9's permanent
  deadline against any unread entry. Key copies need §10's fresh-catch-up
  gate and one-day request-duration assumption: all in-flight copies land
  within one day and five minutes after retirement becomes final, before
  the drain reads. A copy cannot be disregarded after exposure.

  **Ben's laptop removed.** Once its removal has been final for more than
  one day and five minutes, Ana reads its entries, writes and key copies
  to their misses and records that id as drained.
  Her later passes stop probing those logs. **Ana's restored phone:** the
  old id is drained after the same wait following its replacement's finality;
  the new id remains active.
- List `<store>/positions/` once per pass for agreement, retention and peer
  reports. Positions are never an index for discovering logs: upload and
  posting are not atomic. Ana can upload write 9 and crash before posting;
  Ben still finds 9 by GETting its exact name.
- Ben has Ana's writes through 8. GET 9 misses on an idle pass. After Ana
  uploads 9 and 10, GET 9 succeeds, his parallel reads obtain 10 and miss
  at 11, and application still checks both writes' recorded causes.

## 7. Order

Two mechanisms order writes:

- causality decides when a device may apply a write;
- timestamps decide which write a cell keeps.

### 7.1 Causality

- Every write records how far its device had read every other device's
  log.
  - Ana's write 3 had read Ben's log up to 8;
  - and Carol's tablet's log up to 1.
- A device consumes a write only after consuming every position its author
  had read. Consuming a position need not apply its data (§8, §10, §17.1,
  §19.3); the position and the change's verdict are separate facts.
- Every write also records how far its device had read the store log
  ([§9](#9-members-and-roles)), and is applied only after those entries.
  - So a device knows a write's circles, members and keys before it opens
    the write.
  - E.g. Ana's phone makes Gifts and pins note 4 in it. Her tablet holds
    the pin back until it has the entry making Gifts, then opens the pin
    with Gifts' key.
- Resolve the device's stable owner in `store_log_read` and verify with
  that member's current signing key in the replay of that view. A wrong
  signature is a signature refusal (§19.1), not a membership verdict.
- A write counts only if its author was a member and its device one of
  theirs in that recorded view, judged like an entry's
  authority ([§9](#9-members-and-roles)). Otherwise refuse it as
  `NotAuthorized` and block its device's log at that number (§19.1).
  - It creates no lost values: the app must not be able to restore an
    unauthorized author's data.
  - An unknown device registration is a missing prerequisite, not proof
    of missing authority. Read the recorded store-log past before deciding.
  - E.g. Ben's write 9 names the entry removing his phone. Carol refuses
    write 9 and does not pass it or expose its values for restoration.
  - So a write made after its device read its own removal never counts,
    and a write from a device whose addition a later entry drops still
    passes this authority check if its author had read that addition.
    A kept removal or replacement also imposes §10's landing deadline.
- A device has always read its own earlier write positions, including ones
  consumed without applying their effects (§10, §17.1, §19.3). These recorded
  reads govern receive order; actual data sources govern dependence (§8).
- So no device processes an effect before its recorded causes:
  - cause: every write position its device had read when making it;
  - effect: the write's eligible changes;
  - Ana creates note 43 in her write 5. Ben's phone reads it, and Ben adds
    attachment 9 to the note in his write 12.
  - Every device processes Ana's note before Ben's attachment. Dropped data
    follows §8's loss and removal rules instead of being applied.
- Two writes are concurrent when neither device had read the other's:
  - Carol's tablet goes offline before Ben's write 9. Its write 2, at
    14:00, had read Ben's log only up to 8.
  - Ben's write 9 had read Carol's tablet's log only up to 1.
  - So Carol's write 2 and Ben's write 9 are concurrent.
- A device can apply two concurrent writes in either order.
  - Ben's phone applies its own write 9 before Carol's write 2.
  - Carol's tablet applies its own write 2 before Ben's write 9.
  - Neither waits for the other.
  - Both orders converge on the same note.

### 7.2 Timestamps

- Every write carries a timestamp: the device's wall clock time, plus a
  counter.
  - Ana's write 3 is stamped 2026-10-02 12:00:00.000 #0;
  - 12:00:00.000 is the wall clock time, and #0 the counter.
- A timestamp is 48 bits of milliseconds, a 16-bit counter, and the
  device's 64-bit id.
- Timestamps sort by milliseconds, then counter, then device id.
- So no two devices' timestamps are ever equal.
- Every new installation and every device reset gets a new device id (§10).
  Restoring a backup on the same installation can keep its id when custody
  is intact and storage has nothing newer than its reservations.
- The stamping rule:
  - each device keeps the latest timestamp it has seen, from its own writes
    and every write it applies, saved on disk;
  - a write waiting for a cause or a key is not seen until it applies;
  - to stamp a new write:
    - if its wall clock is past that, it uses the wall clock, counter 0;
    - otherwise it uses that latest time, counter raised by one;
    - a counter past its maximum moves to the next millisecond.
  - so a new write is always stamped later than everything its device had
    seen, whatever the devices' wall clocks say.
  - A wall clock before 1970 is never past the latest timestamp, so the
    write takes the latest time with its counter raised.
  - A wall clock past the last time 48 bits hold, in the year 10889, or a
    latest timestamp already at that time with its counter at the maximum,
    fails the write with `DbError::ClockOutOfRange`.
- A write applies once its causes and keys are available, whatever its
  timestamp says about the receiving device's clock.
- Store log entries use the same latest timestamp. Applying an entry
  advances it; an entry never waits for the wall clock either.
- Loading a snapshot adopts its applied writes' timestamps under the same
  rule. Snapshot loading and retention impose no wall-clock hold.
- A far-ahead clock can move other devices' timestamps forward.
  - E.g. Ana's phone jumps a year ahead and edits note 42. Ben's laptop
    applies that edit as soon as its causes arrive.
  - Ben's next edit is stamped after Ana's, even with his clock set right.
  - Carol's concurrent edit, made without reading Ana's and with the
    earlier stamp, loses; its value is recorded as usual (§8).
  - Until real time catches up, devices that have seen the future stamp
    keep using that time with increasing counters. Sync does not freeze.

### 7.3 Example

- Suppose Ana's phone clock runs a minute fast, and Ben's is right. In
  real time:
  - at 13:00:00, Ana edits a note:
    - her phone reads 13:01:00;
    - it stamps her write 13:01:00.000 #0;
    - it uploads the write record.
  - at 13:00:20, Ben's phone downloads Ana's write record:
    - its latest timestamp seen is now 13:01:00.000 #0;
    - it has now read Ana's log up to 4.
  - at 13:00:30, Ben edits the same note:
    - his phone reads 13:00:30, behind 13:01:00.000;
    - it stamps his write 13:01:00.000 #1;
    - it uploads the write record.
- If these were Ana's phone's 4th write and Ben's phone's 9th, storage
  now holds:
  - `<store>/devices/ana-phone/4`:

    ```
    ana-phone, write 4, 2026-10-02 13:01:00.000 #0
      had read: ben-phone 8, carol-tablet 1
      notes  row 42  update  title: "Grocery list" → "Groceries"
    signed with Ana's key
    ```

  - `<store>/devices/ben-phone/9`:

    ```
    ben-phone, write 9, 2026-10-02 13:01:00.000 #1
      had read: ana-phone 4, carol-tablet 1
      notes  row 42  update  title: "Groceries" → "Weekly groceries"
    signed with Ben's key
    ```

- Suppose Carol's tablet, coming back online, downloads Ben's write 9
  before Ana's write 4.
- Causality decides when it applies them:
  - write 9 had read Ana's log up to 4;
  - so Carol's tablet holds write 9 until it has applied write 4.
- Timestamps decide which title the note keeps:
  - same millisecond, but Ben's counter is 1 and Ana's is 0;
  - so Ben's write is later than Ana's.
- Whenever one write had read another, its timestamp is later, as here.
- Timestamps also order concurrent writes, which "had read" can't. The
  merge relies on that.

## 8. Merge

- Merging is how a device applies writes from every device, its own
  included, to build its local database.
- A *cell* is one column of one row: note 42's title is a cell.
- A device applies a write once it has every write that write had read
  ([§7.1](#71-causality)).
- Every device that applied the same writes ends with the same database,
  coven's merge tables included, whatever order the writes arrived in.
- Merging has two layers.
  - The *merged state* comes from the writes themselves: each row's
    generation ([§8.3](#83-deletes)), and each cell's winning value.
  - The *removal rules* then decide which rows the app sees, from the
    merged state and the store log ([§9](#9-members-and-roles)).
- The merged state follows two rules:
  - cells: of two values for one cell, the one with the larger timestamp
    stays;
  - deletes: a row change concurrent with a delete of its row loses
    ([§8.3](#83-deletes)).
- A value is *lost* when some write replaced it, and no write that replaced
  it had read it.
  - A lost value is recorded, so the app can show it and offer it back.
  - Lost values, including a removed row's cells, show what was written;
    deleting a reference's parent does not change them.
  - It stops being lost only when a later write that had read it replaces
    it in place: sets the same cell in the same incarnation of its row, or
    deletes that incarnation. Changing another cell, or the row's next
    incarnation after a delete, leaves it lost
    ([`lostStep`](proofs/merge/CovenMerge/Model.lean)).
- Building on something that does not count does not make it count.
  Dependence is decided by data, never by recorded reads.
  - For each row change, retain the write that supplied its generation and
    the setter of each cell it replaced, as used on the authoring device.
    A row is its table, key and audience. Generation zero has no source.
  - A recorded foreign-key action also depends on the direct changes and
    intermediate actions in the same write that caused it (§8.8). Recompute
    these sources from the original schema and authoring inputs. Each source
    names its write and row, since two changes in one write can have different
    verdicts. Preserve every contributing cause when actions coalesce.
  - An otherwise eligible change is a frozen loss if any such source change
    was dropped. Follow their source changes transitively:
    a change frozen by this rule cannot support another, even if its write's
    other changes survived. An ordinary merge loss does not drop its setter's
    change. One row change, including all its columns, is the unit of loss.
  - Other changes in the same write apply under the ordinary rules. Reading
    a dropped write's position, including an implicit own predecessor,
    creates no data dependence by itself.
  - Record the original change on every device reading that audience: its
    write identity, row, generation, operation, old names and written values,
    with the failed source identities as its cause, generation first, then
    replaced setters in authored order, additional direct-execution sources,
    then foreign-key causes in reconstructed execution order. Keep repeated identities when one
    write supplied several roles, and keep even a delete with no scalar
    values. A later replacement does not acknowledge this loss away.
  - Keep source identities and their per-change verdicts separately from
    dismissible losses, through conversion, snapshots and replay. Snapshot
    coverage records consumed positions; it cannot turn a dropped source
    into retained data. Preserve the original loss cause through later raises.
  - Validate sources against the original authored history. Missing evidence
    waits; invalid evidence is refused. Neither a read position, an equal old
    scalar nor the current winning generation setter establishes the source.
  - Device retirement, member-key replacement, schema exclusion and reset
    supply their own admission rules
    (§10, §17.1, §19.3). A directly dropped retirement write or reset-ignored
    part creates no loss; an otherwise eligible dependent change does.
    [The data rule and value accounting are proved in B17](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data).
- The removal rules take a row out of the app's table while a reason holds:
  - foreign keys: a row whose parent was deleted since the row pointed at
    it is taken out under cascade, restrict and no action, and a row whose
    parent is taken out is taken out with it, under every action
    ([§8.4](#84-foreign-keys));
  - CHECK constraints: a row whose merged values fail is taken out
    ([§8.6](#86-check-constraints));
  - deleted circles: a row in a circle the store log has deleted is taken
    out ([§14.7](#147-deleting-a-circle));
  - unique values: of two rows claiming one value, the row whose write has
    the larger timestamp is taken out, since the first claim keeps it
    ([§8.5](#85-keys-and-uniqueness));
  - keys in two audiences: of two present rows with one key, the store's
    row wins over a circle's ([§14.2](#142-moving-rows)).
- A removal is never stored as a delete.
  - A removed row is recorded as lost while it is out, and its `_coven_lost`
    row keeps its values, which later edits to it update.
  - Coven puts it back from there when the reason goes away, e.g. when the
    reference that made it a parent's child is pointed elsewhere.
  - The app dismissing it ([E4](api.md#e4-reading)) deletes it for good: the
    dismissal write records a delete of the row, so it never comes back,
    and its children follow its foreign keys' delete actions
    ([§8.4](#84-foreign-keys)). A dismissed lost value is dropped from
    `_coven_lost`.
- A removed row's `_coven_lost` row names every rule that holds for it once
  the rules have run, and it comes back only when none holds.
  - E.g. todos need `start <= end`, and todo 7 is in list 3.
  - Ana deletes list 3, while Ben moves todo 7's start past its end.
  - Todo 7 is taken out for both reasons, on every device, whichever rule
    a device ran first.
  - Unique and other-audience losers count their rule as holding, from the
    step that judged it.
- SQLite gives foreign keys and unique constraints no lasting name, so
  coven names them by what they are:
  - a foreign key by its columns, in order, and the table and columns it
    points at, so two keys on one column into different tables stay two;
  - a unique constraint by its terms, in order, each a column's name or an
    expression's text as written, and by its WHERE clause when it is
    partial, so `UNIQUE(title)` and `UNIQUE(lower(title))` stay two;
  - a CHECK by its name, or by its full expression text from the schema when
    unnamed. Normalize by removing outer ASCII whitespace only; preserve
    quoting, comments and internal spacing. Thus `CHECK("n">0)` and
    `CHECK("n"<10)` have distinct identities, `"n">0` and `"n"<10`.
    SQLite may report only the dequoted first token (`n`) for either failure;
    coven evaluates every CHECK on the merged row and records the full
    identities of all failing constraints.
- The app can't see a removed row, so it can insert the same shared key
  again.
  - The write records that insert as an update of the removed row, setting
    every column.
  - The row comes back if the new values clear its reasons, and otherwise
    stays out with them, like any write to a removed row.
  - This is how an app puts a removed row somewhere else, in its audience or
    the store: its reason can still go away, and a copy under a new key
    would then show twice, while the same key shows once (in two audiences,
    the store's row wins).
  - E.g. tag "urgent" is taken out because it fails a CHECK; Ana adds
    "urgent" again with values that pass, and every device puts the row
    back with her values.
  - SQLite can't check against rows that are out, so a local insert can
    still lose.
  - E.g. tags have shared keys and unique labels, and tag b, a child of
    tag a, claimed the label "Plan" first: a loses it, and b goes with a.
  - Adding a again with "Plan" loses the same way, and both stay out.
- Every rule but unique values and keys in two audiences keeps firing when
  more rows are removed, so applying them in any order ends with the same
  rows removed.
- These two are judged once, between two passes of the others:
  1. apply the other rules until none fires;
  2. judge unique values, and keys present in two audiences, among the rows
     still present;
  3. apply the other rules again, to the losers' children.
- So every device that applied the same writes sees the same rows, whatever
  order the writes arrived in, and whatever order the rules ran in.
- The machine-checked merge model ([Appendix B](proofs/merge.md)) must
  establish this for the merged state and every removal rule, including
  rows hidden by a kept circle-deletion entry.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Merging uses these internal tables:
  - `_coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number;
    - the writes it had read.
  - `_coven_columns`, one row per synced column, naming:
    - its table;
    - its column.
  - `_coven_rows`, one row per generation of each synced row, naming:
    - its table;
    - its primary key;
    - its audience;
    - the generation: how many times the row had been created, deleted or
      re-added;
    - the write that moved it there, or of several concurrent ones, the one
      with the smallest timestamp.
  - `_coven_cells`, one row per synced cell, naming:
    - its `_coven_columns` row;
    - its `_coven_rows` row;
    - the write that set it.
  - `_coven_references`, one row per reference a synced cell holds, naming:
    - the cell's `_coven_rows` and `_coven_columns` rows;
    - its `_coven_foreign_keys` row;
    - the parent's table, key, audience, and the generation it points at
      ([§8.4](#84-foreign-keys)).
  - `_coven_foreign_keys`, one row per foreign key of a synced table, naming
    it as below.
  - `_coven_claims`, one row per unique value a removed row claims, naming:
    - the row's `_coven_rows` row;
    - its `_coven_constraints` row, which names the unique constraint as
      below;
    - the row's audience and the value it claims
      ([§8.5](#85-keys-and-uniqueness)).
  - A present row's displayed values are in the app's table. When a
    reference reads differently from what was written (§8.4),
    `_coven_reference_values` keeps the written value by cell until the
    two agree again.
  - `_coven_lost`, one record for every kind of loss, naming:
    - the table, key and audience;
    - the column for a cell, or no column for a whole row;
    - the incarnation, or an excluded row change's generation;
    - the value that lost, or every value of the removed row;
    - the write that set each value;
    - what replaced it: a write that hadn't read it, the rules that removed
      the row, a breaking change that excluded it, dropped data sources,
      or a refused owner permission or foreign-key derivation (§8.8);
    - whether its values are frozen: unauthorized changes, dependent changes,
      schema-excluded writes
      and rows deleted by a breaking migration retain old-shape values
      ([§17.1](#171-host-application));
    - for an excluded or dependent change, its write identity and operation,
      even if a deletion has no old values.
    All losses use this record, one snapshot section and one fingerprint
    leaf shape. Required source evidence is retained independently of these
    loss records; dismissing one cannot change a source's verdict.
- Store-log publication uses `_coven_store_log_uploads`: the next local entry's
  number, canonical plaintext record and sealing key id.
- `_coven_key_uploads` is the one sealed-copy queue. It holds each writer's
  reserved copy number, fixed sealed bytes, and an optional `before_entry: EntryId`. A set
  entry means the copy must be confirmed before publishing that entry; no
  entry means historical sharing (§11). The entry and its prerequisite copies
  commit together before any storage attempt. Reserving a copy commits its
  number and fixed bytes as an irrevocable attempt; publication follows number
  order even if its initiating operation is discarded. There are no unsealed keys here.
  Confirmed copies leave this queue; applying a published entry and its replay
  removes its entry row atomically (§9, §18). Occupancy of this writer's path
  requires §10's byte comparison, not an assumption that another writer won.
- `_coven_key_publications` retains each writer's last confirmed copy number.
  Confirmation advances it atomically with removing that copy's queue row,
  so an empty queue cannot restart the writer's numbering.
- Received store-log entries and key copies are retained apart from what
  replay applied (§6). `_coven_log_inputs` keeps each complete object read by
  its exact number, with its provider observation, including objects waiting
  for keys or causes and fetch-ahead reads past a gap.
  `_coven_log_downloads` holds one contiguous received number per writer and
  log, advanced in the transaction that retains the input reaching it, and
  zero for a discovered writer with nothing received yet. A miss or failed
  read never advances it, and it never substitutes for applied positions.
- `_coven_catch_up` holds §15's return checkpoint `S`, absent until a
  catch-up first completes with its positions confirmed posted. It only moves
  forward; an unchanged time writes nothing.
- The store log's effects that the database applies are kept with it:
  - `_coven_circle_deletions` records each deleted circle's responsible kept
    entry; local writes, downloaded writes and row recomputation all read
    deletion from that same table
    ([§9](#9-members-and-roles), [§14.7](#147-deleting-a-circle));
  - `_coven_boundaries`, keyed by the boundary's store-log entry, keeps its
    audience, kind (raise or reset), snapshot schema version, snapshot id,
    and verified write/store-log coverage. Late writes and reversal use this
    same record (§17.1, §19.3). The current reset and raise per audience are
    selected from kept entries in `_coven_store_log`, not duplicate tables.
    Superseded boundaries remain while any non-final effect needs them.
  - Ana's raise and Ben's concurrent reset each have one boundary row. Replay
    chooses the kept entry; if that choice changes, the retained rows supply
    the exact snapshot coverage without reconstructing a second history.
- Fingerprints ([§19.1](#191-noticing)) are kept incrementally:
  `_coven_fingerprint_leaves` holds one hash per row and per loss in
  each audience, and `_coven_fingerprint_sums` their sum per audience, so
  a write updates only the hashes of the rows it changed.
- Note 42 on Ben's phone, after Ana's write 4 and its own write 9:

  ```
  notes                            the app's own table
    id   title               body
    42   "Weekly groceries"  "milk, eggs"

  _coven_columns
    id   table   column
    1    notes   title
    2    notes   body

  _coven_rows
    id   table   key   audience   generation   write
    3    notes   42    store      1            1

  _coven_cells
    column   row   write
    1        3     7
    2        3     1

  _coven_writes
    id   timestamp                                number
    1    2026-10-01 09:12:40.511 #0  ana-phone    1
    7    2026-10-02 13:01:00.000 #1  ben-phone    9
  ```

- To find which write set note 42's title:
  - in `_coven_columns`, notes' title is column 1;
  - in `_coven_rows`, notes row 42 is row 3;
  - in `_coven_cells`, column 1 of row 3 points to write row 7;
  - in `_coven_writes`, write row 7 is Ben's phone's write 9, stamped
    13:01:00.000 #1.

### 8.1 Example

- Carol's tablet goes offline at 12:30, having read Ana's log up to 3 and
  Ben's up to 8.
- At 13:00 Ben sets the title to "Weekly groceries". His phone commits this
  as its write 9.
- At 14:00 Carol sets the title to "Shopping". Her tablet commits this as
  its write 2:

  ```
  carol-tablet, write 2, 2026-10-02 14:00:00.000 #0
    had read: ana-phone 3, ben-phone 8
    notes  row 42  update  title: "Grocery list" → "Shopping"
  signed with Carol's key
  ```

- At 14:30 the tablet comes back online and uploads the write record to
  `<store>/devices/carol-tablet/2`.
- Carol's tablet never read Ben's write 9, and Ben's phone never read
  Carol's write 2. The two writes are concurrent, so only their
  timestamps can order them.
- When a device applies one of these writes to the title, it:
  - adds the write's row to `_coven_writes`;
  - compares the write's stamp with the stamp of the title's current write;
  - if the new stamp is larger, sets the title in `notes` and points the
    title's `_coven_cells` row at the new write.
- Each device has its own `_coven_writes`, so one write gets a different row
  on each device.
- Ana's write 4, for example, is row 6 on Ben's phone and row 14 on Carol's
  tablet.
- Note 42's title on each device, in real time.

Ben's phone:

<table>
  <tr><th>Time</th><th>Applies → <code>_coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>_coven_cells</code> row → <code>_coven_writes</code> row</th></tr>
  <tr><td>13:00</td><td>Ana's write 4 → 6</td><td>13:01:00 #0</td><td>12:00:00 #0</td><td>yes</td><td>"Groceries"</td><td>6</td></tr>
  <tr><td>13:00</td><td>its write 9 → 7</td><td>13:01:00 #1</td><td>13:01:00 #0</td><td>yes</td><td>"Weekly groceries"</td><td>7</td></tr>
  <tr><td>14:30</td><td>Carol's write 2 → 8</td><td>14:00:00 #0</td><td>13:01:00 #1</td><td>yes</td><td>"Shopping"</td><td>8</td></tr>
</table>

Carol's tablet:

<table>
  <tr><th>Time</th><th>Applies → <code>_coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>_coven_cells</code> row → <code>_coven_writes</code> row</th></tr>
  <tr><td colspan="7"><em>12:30 · goes offline</em></td></tr>
  <tr><td>14:00</td><td>its write 2 → 13</td><td>14:00:00 #0</td><td>12:00:00 #0</td><td>yes</td><td>"Shopping"</td><td>13</td></tr>
  <tr><td colspan="7"><em>14:30 · comes back online and uploads its write 2</em></td></tr>
  <tr><td>14:30</td><td>Ana's write 4 → 14</td><td>13:01:00 #0</td><td>14:00:00 #0</td><td>no</td><td>"Shopping"</td><td>13, unchanged</td></tr>
  <tr><td>14:30</td><td>Ben's write 9 → 15</td><td>13:01:00 #1</td><td>14:00:00 #0</td><td>no</td><td>"Shopping"</td><td>13, unchanged</td></tr>
</table>

- Ben's phone and Carol's tablet applied the same three writes in opposite
  orders:
  - Ben's phone was online, so it applied Ana's write and its own at 13:00,
    and Carol's at 14:30;
  - Carol's tablet was offline, so it applied its own write first, and
    Ana's and Ben's at 14:30.
- Both devices end with "Shopping": each cell holds the largest-stamped
  write applied, whatever the order.

### 8.2 Concurrent writes to one row

- Two concurrent writes to one row can overlap or not:
  - a cell both set goes to the write with the larger timestamp ([§8.1](#81-example));
  - a cell only one sets keeps that write's value.
- All cells a write sets share its timestamp, so between any two writes,
  the one with the larger timestamp wins every cell they both set.
- At 15:00, offline, Ben sets note 42's title and Ana sets its body:

  ```
                    title                body
  Ana's write 6     ·                    "milk, eggs, bread"
  Ben's write 10    "Weekend shopping"   ·
  result            "Weekend shopping"   "milk, eggs, bread"
  ```

- No cell overlaps, so both writes keep their cells on every device.
- When concurrent writes set the same cell, the newer one wins.
- The value it replaced is lost when no write that replaced it had read it
  ([§8](#8-merge)).
- In [§8.1](#81-example), note 42's title went through four values:

  ```
  value               set by           replaced by             lost?
  "Grocery list"      Ana's write 3    Ana 4, Ben 9, Carol 2   no: all had read it
  "Groceries"         Ana's write 4    Ben 9, Carol 2          no: Ben 9 had read it
  "Weekly groceries"  Ben's write 9    Carol 2                 yes
  "Shopping"          Carol's write 2  nothing                 the current value
  ```

  - On Carol's tablet, Ana's write 4 arrives after Carol's write 2, so
    "Groceries" is lost at first; Ben's write 9, which had read it, then
    removes its `_coven_lost` row.
  - Every device ends with one row:

    ```
    _coven_lost
      cell            lost value          set by          replaced by
      note 42 title   "Weekly groceries"  Ben's write 9   Carol's write 2
    ```

- The app reads lost values through `lost_values`
  ([E4](api.md#e4-reading)) and can offer to restore one.
- Every device holds the same `_coven_lost` rows, because they follow from
  the writes alone.

### 8.3 Deletes

- A row's *generation* counts how many times it has been created, deleted
  or re-added.
  - It is odd while the row exists and even while it is deleted.
  - Each generation has its own `_coven_rows` row, naming the write that
    moved the row there.
- Each row change in a write record carries the generation the row had on
  the device that wrote it:

  ```
  ben-phone, write 11, 2026-10-02 16:00:00.000 #0
    had read: ana-phone 6, carol-tablet 2
    notes  row 43  generation 1  update  title: "Hardware store" → "Hardware store, Saturday"
  signed with Ben's key
  ```

- Edits to a row's columns change only its `_coven_cells` rows, never its
  generation.
- Deleting a row removes it from the app's table and moves its generation
  on to the next even number.
- Re-adding it moves its generation on to the next odd number.
- Its `_coven_cells` rows go with it, but its `_coven_rows` rows stay, so
  later writes to it still have a generation to lose to.
- A row change concurrent with a delete of its row loses, and its cells go
  to `_coven_lost`, replaced by the delete.
  - If the change arrives after the delete, coven sees it was made at an
    older generation.
  - If it arrives first, the delete records the cells set by writes it
    hadn't read, before the row goes.
  - So a concurrent edit never brings the row back, and never reaches it
    once it is re-added.
- When two deletes of one generation are concurrent, a value is lost only
  if neither had read it, by the rule of [§8](#8-merge).
  - The first delete to arrive records what it hadn't read.
  - If the second had read one of those values, it removes that value's
    `_coven_lost` row, which needs nothing from the deleted row.
  - The generation's `_coven_rows` row names the delete with the smaller
    timestamp, whichever arrived first.
  - E.g. Carol retitles note 43, and Ana and Ben, both offline, delete it;
    Ben had read Carol's edit and Ana hadn't.
  - A device that gets Ana's delete first records Carol's title as lost,
    and removes that `_coven_lost` row when Ben's delete arrives.
  - Every device ends with note 43 deleted and Carol's title not lost.
- E.g. at 16:00 Ana deletes note 43, "Hardware store", while Ben, offline,
  edits its title, and at 17:00 Ana re-adds it.
  - Note 43 on Carol's tablet:

    ```
    _coven_rows
      time    id   table   key   audience   generation   write
      14:45   4    notes   43    store      1            16      Ana's write 5 creates it
      16:00   5    notes   43    store      2            19      Ana's write 7 deletes it
      17:00   6    notes   43    store      3            20      Ana's write 8 re-adds it

    notes
      14:45   row 43 present
      16:00   row 43 gone
      17:00   row 43 present again
    ```

  - Ben's edit was made at generation 1, so it loses whenever it arrives,
    even if a fast clock stamps it after 17:00.
  - Generation 2's row names Ana's write 7, so even a device that gets
    Ben's edit after the re-add records it as replaced by write 7.
  - Every device records:

    ```
    _coven_lost
      cell           lost value                  set by          replaced by
      note 43 title  "Hardware store, Saturday"  Ben's write 11  Ana's write 7
    ```

- Concurrent deletes both move the generation from 1 to 2, so they count
  as one delete.
- If Ben had deleted note 43 instead of editing it, Ana's 17:00 re-add
  would still bring it back, since Ben deleted the same generation she did.
- Concurrent re-adds both move it from 2 to 3, and their cells merge
  ([§8.2](#82-concurrent-writes-to-one-row)).
- Between 14:45 and 16:00, Carol's tablet applies two writes that leave
  note 43's generation as it is:
  - Ana's write 6, her 15:00 edit to note 42's body
    ([§8.2](#82-concurrent-writes-to-one-row)), as row 17;
  - Carol's own write 3, at 15:30, which adds attachment 8 to note 43
    ([§8.4](#84-foreign-keys)), as row 18.

### 8.4 Foreign keys

- A foreign key makes one row point at another, its parent: e.g.
  attachment 9's `note_id` points at note 43, so note 43 is attachment 9's
  parent.
- For changes in different writes, every device consumes the parent's
  insert position before processing a change that points at it, through
  causality alone ([§7.1](#71-causality)). When both changes are retained,
  it applies them in that order:

  ```
  Ana's phone    write 5: insert note 43
                    │
                    │  Ben's phone applies write 5, then Ana's write 6
                    ▼
  Ben's phone    write 12: insert attachment 9, pointing at note 43
                 had read: ana-phone 6
                    │
                    │  another device downloads Ben's write 12 first
                    ▼
  that device    holds Ben's write 12
                    → applies Ana's write 5, inserting note 43
                    → applies Ana's write 6, and Ben's earlier writes
                    → applies Ben's write 12, inserting attachment 9
  ```

- A row change that points at a parent also carries the parent's
  generation ([§8.3](#83-deletes)).
- A reference spanning several columns merges as one value. Changing any
  component records every component, including unchanged values, and the
  same parent identity and generation on each component. The write with the
  larger timestamp wins the whole tuple, including a tuple containing NULL.
  - Overlapping keys expand together until every touched key is complete.
    Generated components include their input columns in this group, so SQL
    cannot recompute a different tuple from independently merged inputs.
  - For `FOREIGN KEY(a,b)`, concurrent changes from `(A,3)` to `(B,3)` and
    `(A,5)` yield either `(B,3)` or `(A,5)`, never `(B,5)`.
- On the device that deletes a parent, SQLite runs each child's foreign key
  action, and the write records the results as derived row changes with their
  causes (§8.8). They use the ordinary generation and cell merge rules.
  - E.g. attachments point at notes with cascade, and links with set null;
    Carol added attachment 8 to note 43 in her write 3 at 15:30, and every
    device has it.
  - At 16:00 Ana deletes note 43, and her write records:

    ```
    ana-phone, write 7, 2026-10-02 16:00:00.000 #0
      had read: ben-phone 9, carol-tablet 3
      notes        row 43  generation 1  delete
      attachments  row 8   generation 1  delete
      links        row 5   generation 1  update  note_id: 43 → null
    signed with Ana's key
    ```

- A `RESTRICT` or `NO ACTION` key makes SQLite refuse that delete instead,
  if the deleting device has a child.
- A child the deleting device didn't have, because it was added or pointed
  at the parent concurrently, points at a generation that is deleted, on
  every device:
  - under cascade, restrict or no action, the removal rule of [§8](#8-merge)
    takes it out of the app's table and records it as lost;
  - under set null, it stays, and its reference is null, as SQLite would
    have made it.
  - The app's table reads the reference as null. Lost values always show
    what was written, without this substitution.
  - The cell still names the write whose reference won; it only reads as
    null, and later writes to it compete with that write's timestamp, as
    with any cell. Nothing is recorded as lost.
  - Coven's merge records keep the live reference as written, and its parent
    generation: the cell reads as null while that generation is deleted,
    and as written again if a reset brings the generation back
    ([§19.3](#193-resetting-a-store)). A later concurrent write that
    displaces it records the written value as lost, whichever write arrived
    first. Snapshots carry the reference as written.
  - E.g. links point at notes with set null, and three writes happen:

    ```
    16:00  Ben adds link 6 → note 43
    16:10  Dan, having read Ben's write, points link 6 at note 44
    16:20  Ana, having read neither, deletes note 43
    ```

  - A device that gets Ana's delete and Ben's insert first reads link 6
    as null, with the cell still naming Ben's write.
  - Dan's write then wins over Ben's, so every device ends with link 6 on
    note 44, whatever order the writes arrived in.
- Foreign keys of synced tables must reference the parent's complete primary
  key. An explicit column list may reorder those columns; an omitted list
  means primary-key order. A UNIQUE non-key column, a subset of a composite
  primary key, or repeated target columns is refused on open and after
  migrating with `SchemaError::ForeignKeyNotPrimaryKey`, naming the table
  and foreign key ([Appendix A.9](#a9-references-to-non-key-columns)).
- Coven refuses `ON DELETE SET DEFAULT` and `ON UPDATE SET DEFAULT` on
  any foreign key of a synced table, and on a foreign key from a local
  table into a synced table, checked when the database opens and after
  migrating. The error names the table and foreign key
  ([Appendix A.8](#a8-set-default-foreign-keys)).
- Coven refuses set null on a `NOT NULL` column in any table, checked when
  the database opens and after migrating.
  - SQLite can never apply such an action: no device could delete the
    parent, and coven couldn't take it out.
- Where setting the reference to null would fail a CHECK, the child is
  taken out as under restrict.
- Coven takes rows out with SQLite's foreign keys enforced, children
  first, so SQLite's own actions reach only local children.
- Before an entry-dependent removal changes local children, coven retains
  their rows and reference values. Those actions are derived effects, not
  app edits.
  - If replay reverses the removal, restore the children and references.
    Apply any explicit local edits made since against the retained values;
    a later app delete or replacement is not undone.
  - Keep these inputs until the responsible entries are final (§9).
  - E.g. Carol's local pin points at note 7 in Gifts. A provisional circle
    deletion hides both. If the deletion drops, the note and pin return.
    If Carol explicitly deleted that pin meanwhile, it stays deleted.
- A synced row coven deletes or takes out can have children that only this
  device has, in local tables, so a local foreign key must never stop it:
  - coven refuses, checked when the database opens and after migrating, a
    foreign key from a local table into a synced table, or into a local
    table one of these reaches, unless its delete action is cascade, or
    set null on a column that allows NULL;
  - e.g. `pins(note_id REFERENCES notes ON DELETE RESTRICT)` is refused:
    Ben's phone, holding a pin, couldn't take out a note that Ana's phone
    could, and the two would differ.
- Anything else in the app's local schema that refuses such a delete, such
  as a trigger that raises an error, fails the write or the apply that
  needs it on that device, until the app changes its schema.
- A child whose parent is taken out by a rule, rather than deleted, is
  taken out with it under every action, and comes back with it.
- E.g. note 46 loses its title to note 45 and is taken out; link 7, which
  points at note 46 with set null, is taken out with it, not set to null.
- Coven refuses set null on a primary key column, checked when the database
  opens and after migrating, since nulling a key column changes the row's
  key: a delete plus an insert no write recorded.
- A taken-out child's own generation never moves, so it comes back if its
  reference is later pointed at a parent that is present.
- E.g. at 16:00, while Ana deletes note 43, Ben, offline, adds attachment 9
  to it, then moves attachment 9 to note 44:

  ```
  Ben's phone
    16:00  offline. Ben's write 12: insert attachment 9 → note 43
    16:05  Ben's write 13: attachment 9 → note 44
    16:30  online. Applies Ana's write 7: delete note 43
             attachment 9 points at note 44, so no rule applies

  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
    16:30  Ben's write 12 arrives: attachment 9 → note 43
             note 43 is gone, so attachment 9 is taken out, and recorded as lost
           Ben's write 13 arrives: attachment 9 → note 44
             the reason is gone, so attachment 9 is put back
  ```

  - Both devices end with attachment 9 on note 44, and nothing lost.
  - Had Ben not moved it, both would end with attachment 9 taken out, and
    the same `_coven_lost` row, so the app can offer to put it on another
    note, by inserting it again with its key ([§8](#8-merge)).
- Re-adding a deleted parent doesn't bring back the rows taken out with it
  under cascade, restrict or no action, since they point at its old
  generation.
- Re-adding is a new insert. Any children the app wants back, it inserts in
  the same write.

### 8.5 Keys and uniqueness

#### Kinds of keys

- Each synced table declares one of two kinds of primary key:
  - independent: each new row gets a UUID, so rows made on different
    devices never share a key;
  - shared: the app derives the key from what makes the row unique, so
    equal values are one row on every device.
- Coven refuses, checked when the database opens and after migrating:
  - an independent key with no text column to hold its UUID;
  - a key SQLite picks itself, such as an integer rowid;
  - a synced table with no primary key;
  - a primary key column that allows NULL, since SQLite lets a non-integer
    key column hold NULL unless it is declared NOT NULL, and a key with a
    NULL in it names no row.
- Either kind can span several columns.
  - E.g. `note_tags(note_id, tag_id)` is a shared key.
  - Ana and Ben, both offline, each tag note 42 "urgent", and make one row.
  - An independent key over several columns needs a UUID in one of them.
  - A write that inserts a row whose independent key holds no UUID,
    version 4 or 7 in canonical lowercase form, is refused; opening never
    reads the rows to check.
- E.g. notes have independent keys, and tags have shared keys derived from
  the tag's name.
  - Ana and Ben, both offline, each add the tag "urgent".
  - Both derive the same key, so their inserts are one row, and merge
    ([§8.2](#82-concurrent-writes-to-one-row)).
- With shared keys, a device can insert a row another device is deleting.
  - E.g. Ana deletes the tag "urgent" while Ben, offline and never having
    received it, adds "urgent".
  - Ben's insert, made at generation 0, would start generation 1, which
    Ana's delete has ended, so it loses ([§8.3](#83-deletes)), and his
    cells go to `_coven_lost`.

#### Key changes

- A primary key change is a delete of the old row plus an insert of the
  new one, on every device.
- On the device changing the key, SQLite runs each child's `ON UPDATE`
  action, and the write records the results as ordinary row changes.
- A child that device didn't have sees its parent deleted, and follows its
  `ON DELETE` action by the removal rule of [§8.4](#84-foreign-keys).
- E.g. `note_tags(note_id, tag_id)` points at tags with `ON UPDATE
  CASCADE` and `ON DELETE CASCADE`, and at 16:00 Ana renames the tag
  "urgent" to "important", while Ben, offline, tags note 44 "urgent".

  ```
  Ana's phone
    16:00  renames "urgent" to "important"
             SQLite changes note_tags (42, "urgent") to (42, "important")
             her write: delete tag "urgent", insert tag "important",
                        delete (42, "urgent"), insert (42, "important")

  Ben's phone
    16:00  offline. Ben's write: insert note_tags (44, "urgent")
    16:30  online. Applies Ana's write
             "urgent" is deleted, so (44, "urgent") is taken out

  Carol's tablet
    16:00  applies Ana's write
    16:30  Ben's write arrives: insert note_tags (44, "urgent")
             "urgent" is deleted, so (44, "urgent") is taken out
  ```

- Every device ends with note 42 tagged "important", and note 44's
  "urgent" tag taken out and recorded in `_coven_lost`, so the app can offer
  to tag it again.
- Two devices changing one key concurrently are two concurrent deletes of
  its generation ([§8.3](#83-deletes)), and both new rows exist after the
  merge.

#### Unique constraints

- On one device, SQLite refuses a write that repeats a unique value, so two
  rows can claim one value only through concurrent writes.
- Of two rows claiming one value, the row whose write has the smaller
  timestamp keeps it, since the first claim to a value keeps it.
- The other row is taken out, whether its write inserted it or edited it
  to claim the value, and recorded as lost.
  - A change to its own value that ends the conflict clears that reason.
  - A loser whose value is unchanged comes back only when the winner is
    deleted, or taken out before unique values are judged.
  - Unique values are judged after the other removal rules, among the rows
    they leave ([§8](#8-merge)).
- E.g. note titles are unique, and Ana and Ben, both offline, each add a
  note titled "Groceries".
  - Ana's insert of note 45 is stamped 16:00, and Ben's of note 46 is
    stamped 16:05.
  - Every device keeps note 45, and records Ben's note 46 in `_coven_lost`.
- A row's claim dates from the latest write that set any column the
  constraint reads in it, since that is when the row first held the whole
  value.
  - The constraint reads its columns, the columns its expressions use and,
    for a partial index, the columns its WHERE clause uses.
  - A generated column counts as set by each write whose changes to its
    inputs changed its value.
  - E.g. notes are unique by `(folder, title)`, and note 2 is "Draft" in
    Home.

    ```
    10:00  Ana adds note 1: "Plan" in Work
    11:00  Ben renames note 2 to "Plan"
    12:00  Carol, not having seen Ben's write, moves note 2 to Work
    ```

  - After the merge, note 2 is "Plan" in Work, like note 1.
  - Note 2's claim dates from 12:00, and note 1's from 10:00, so note 1
    keeps the value and note 2 is taken out.
- Of two claims with the same timestamp, the row with the smaller primary
  key keeps the value.
- A winner taken out after unique values are judged doesn't bring its
  loser back ([§8](#8-merge)).
  - E.g. notes are unique by title, and a sub-note points at its parent
    note with cascade.

    ```
    09:00  note 1 is "Ideas"
    10:00  Ana adds note 2, "Plan", as a sub-note of note 1
    11:00  Ben, not having seen note 2, renames note 1 to "Plan"
    ```

  - After the merge, note 2's claim from 10:00 keeps "Plan", and note 1 is
    taken out.
  - Note 2's parent is taken out, so note 2 goes too, by cascade.
  - Note 1 doesn't come back: if it did, note 2 would come back with it
    and take it out again.
  - Both are recorded in `_coven_lost`, so the app can offer them back.

### 8.6 CHECK constraints

- A CHECK that reads one stored column can't fail after a merge, since
  each value passed it on the device that wrote it.
- A CHECK that reads several stored columns can, when concurrent writes
  each set some of them. A generated column counts as the stored columns it
  is computed from, so a CHECK on one generated column can fail too.
- E.g. a row checks `start <= end`, Ana sets `start` and Ben, offline, sets
  `end`:

  ```
                start   end
  before         5      12
  Ana's write   10       ·
  Ben's write    ·       8
  merged        10       8     fails
  ```

- A row that fails after a merge is taken out, and recorded as lost.
- It comes back if a later write makes it pass, e.g. Ben setting `end` to
  20.
- Until the second write arrives, each device's row passes, since it has
  seen only one of them.
- Every device ends with the row taken out, and the same `_coven_lost` row.

### 8.7 Triggers

- The app declares each trigger on a synced table as local or shared;
  a trigger it doesn't declare shared is local ([E2](api.md#e2-declaring-synced-tables)).
- A local trigger runs on every device, for its own writes and applied ones
  alike, and writes only local tables.
  - Every change coven makes is ordinary SQL, including a late write
    taking a row back out ([§8](#8-merge)), so a count a local trigger keeps stays
    right.
  - E.g. a local trigger keeps a search index of note titles:

    ```sql
    CREATE TRIGGER notes_title_index AFTER UPDATE OF title ON notes
    BEGIN
      UPDATE title_index SET title = new.title WHERE note_id = new.id;
    END;
    ```

  - When Ben's phone applies Carol's write 2, the trigger updates Ben's
    index to "Shopping".
- A shared trigger runs only on the device making the write, writes only
  synced tables, and its writes become part of that write.
  - Its targets are direct writes for owner permissions (§8.8), including
    when a foreign-key action fires the trigger. Only the foreign-key action
    itself receives the derived-change exception.
  - E.g. a shared trigger sets a note's `edited_at` when its body changes:

    ```sql
    CREATE TRIGGER notes_edited_at AFTER UPDATE OF body ON notes
    WHEN NOT coven_applying()
    BEGIN
      UPDATE notes SET edited_at = datetime('now') WHERE id = new.id;
    END;
    ```

  - Ana's write 6 records her edit and the trigger's:

    ```
    ana-phone, write 6, 2026-10-02 15:00:00.000 #0
      had read: ben-phone 9, carol-tablet 2
      notes  row 42  generation 1  update  body: "milk, eggs" → "milk, eggs, bread"
      notes  row 42  generation 1  update  edited_at: → 2026-10-02 15:00    shared trigger
    signed with Ana's key
    ```

  - Ben's phone applies both without running the trigger.
- Coven skips shared triggers when it applies another device's write,
  since the write already holds what they did.
  - SQLite can't turn off one trigger, so coven provides the SQL function
    `coven_applying()`, true while it applies another device's write.
  - A shared trigger declares `WHEN NOT coven_applying()`, as above, and
    coven refuses one that doesn't, checked when the database opens and
    after migrating.
- A trigger's write to the wrong kind of table fails.
  - SQLite's authorizer callback reports each table a statement would
    write, with the trigger doing the write, when the statement is
    prepared.
  - Coven refuses the statement when a local trigger writes a synced table,
    or a shared trigger a local one.
- A shared trigger's writes merge like any other write, so a value it
  derives can be wrong after concurrent writes.
  - E.g. a shared trigger counts a note's attachments, and Ana and Ben,
    both offline, each add an attachment to note 42:

    ```
                  attachments on note 42   attachment_count
    Ana's write   adds attachment 10       1 → 2
    Ben's write   adds attachment 11       1 → 2
    merged        3 attachments            2
    ```

  - A local trigger writing a local table counts 3 on every device.

### 8.8 Owner tables

- A synced table may declare `owner_only()` ([E2](api.md#e2-declaring-synced-tables)).
  Its owner permission uses coven's retained creator identity, not a mutable
  app column. Tables without this declaration have no row-owner restriction.
- A row's owner is the authenticated member who originally created it.
  Inserting a new row establishes that author as its owner. Updating,
  deleting, reinserting or copying it between audiences preserves that identity.
  - Concurrent generation-zero creations of one shared key select the
    creating write with the smallest timestamp, before permission filtering,
    on every device. First receipt cannot choose the owner. If an earlier
    competing creation arrives later, recheck the affected changes against
    the selected creator.
  - Retain the creating write's authenticated identity through deletion,
    loss dismissal, schema conversion and snapshots. Renaming a table or
    key does not make the migration's author the row's creator.
- Only the owner may directly update or delete an owned row. An insert over
  an existing or removed shared key is an update for this permission.
  - Direct targets include the write's own statements and its shared
    triggers. Check every direct target even if its effects combine with a
    foreign-key action on the same row. A trigger cannot borrow permission
    from the row that fired it.
  - At commit, a foreign direct target refuses the whole local transaction
    with `DbError::NotOwner`, leaving its rows and upload queue unchanged.
  - On receipt, the unauthorized row change becomes a frozen per-change loss
    naming the owner; unrelated changes in the write still apply. Changes
    that used its data follow §8's dropped-source rule. This is separate from
    the whole-write membership and signature checks in §7.1.
- A foreign-key action derived from an authorized direct change in the same
  write can update or delete another owner's row. This includes transitive
  CASCADE and SET NULL actions, on deletion or referenced-key updates.
  - Receivers recompute the complete effect from the original schema, the
    write's direct changes and verified authoring inputs. They do not trust
    a claimed direct/derived label, target, generation, scalar patch or cause
    list. Shared-trigger effects remain direct targets.
  - Match the exact row, generation, operation and every changed column and
    value, including all components of a compound key. Several actions on
    one row must match their complete combined effect in original execution
    order; accepting only one component is not sufficient.
  - Every derivation must reach authorized direct causes in this write. A
    cycle with no direct cause proves nothing. A proposed foreign-owned
    change without this exact derivation is an unauthorized per-change loss;
    missing prerequisite evidence waits instead of granting permission.
  - Keep the verified derivation, full original row change and all contributing
    causes independently of dismissible losses. Keep a direct action and
    its data sources even when its own row ends with no net change: its
    foreign-key effects can remain. An accepted cascade is a
    recorded derived delete even when it knowingly replaced the old values
    and the ordinary cell rules require no lost-cell record.
  - If a cause drops, its derived row changes drop through §8's data rule.
    Replay the surviving changes; the row's earlier generation and values
    return unless an independent surviving change or removal rule keeps them
    out. A combined row change loses if any contributing cause loses.
- All ordinary schema and removal rules apply to owner tables. UNIQUE and
  CHECK conflicts can hide owned rows, and shared triggers can cause such
  conflicts through their authorized targets. Every hidden row has its
  ordinary loss naming the causes; when the last cause disappears, it returns.
  The existing SET DEFAULT refusal (§8.4) also applies.
- E.g. Ben deletes a track while Ana's review points at it. With CASCADE,
  the write records the review's derived deletion. With RESTRICT, SQLite
  refuses the delete while the review references it. Ben can never directly
  edit Ana's review.
- Owner permissions and verified foreign-key consequences are checked on
  every device from the same authenticated inputs, including after a reload.
  [B18](proofs/merge.md#b18-owner-tables-and-row-authenticity) proves their
  authorization, agreement, loss recording and reversal at the stated SQL
  extraction boundary. Ownership grants no additional schema restrictions.

## 9. Members and roles

- A member is a person in the store, using it from one or more devices.
  - Each has a random 16-byte UUID, `MemberId`, independent of their keys,
    like a circle id. Device ownership and circle membership name that id.
  - Each member has current signing and sealing keys ([§11](#11-keys)).
    An admin adds the id and both public keys to the store log.
- The *store log* records changes to the store itself, separate from the
  app's writes.
  - Each change is one *entry*: add or remove a member, change a role, add
    or remove a device, replace a member's keys, make, rename or delete a circle
    ([§14](#14-audiences)) or change its members, rotate an audience's key,
    raise the store's or a circle's schema version, or reset the store or a circle to a
    snapshot ([§15](#15-snapshots)).
  - An entry names the store log entries its author had read, and is
    signed with the current signing key of its `author:MemberId` in that
    recorded view. Retain historical public bindings to check older objects.
  - Entries live at `<store>/store-log/<device>/<n>`, numbered like a device's
    writes ([§6](#6-syncing-writes)).
  - Every entry is a single-chunk object of at most 4 MiB, published in one
    request; entries never use recorded upload sessions. Finality relies on
    this: the provider timestamp of a one-request publication is its completion
    time. Multipart data writes and files do not establish entry finality.
  - The device in the path is only where the entry was written from.
  - Each device numbers its own entries, so no two entries ever get the
    same path. There is no shared sequence or cross-device slot to claim.
  - Verify the signature against that stable author's keys in the replay of
    `had_read`, including implicit own entries. The device path is not an
    alternative author. Creation supplies its author's initial pair in its
    payload and its id in the header; verify both the signature and store path.

    ```
    <store>/store-log/ana-phone/1      create the store, Ana as admin       signed with Ana's key
    <store>/store-log/ana-phone/2      add Ben's id and public key pair    signed with Ana's key
                               had read: ana-phone 1
    <store>/store-log/ben-laptop/1     add device ben-phone                 signed with Ben's key
                               had read: ana-phone 2
    <store>/store-log/ana-ipad/1       make Ben an admin                    signed with Ana's key
                               had read: ana-phone 2
    ```

  - The store's first entry creates it, names its first admin, and adds
    the device that wrote it, with its name.
- Administrative actions require storage to be reachable. Before starting
  membership, role, access, circle or reset work, catch up on the store log
  and run the candidate through the same authority and replay rules against
  that view. Return `SyncError::Rejected(DropReason)` if they reject it;
  validation has no separate membership-rejection vocabulary.
  - Without storage, or if the catch-up fails, return the typed failure
    without reserving an entry or starting an operation.
  - An already-started operation keeps its durable progress after a
    connection failure; its immutable attempted entries retry while the
    current-credential send gate permits them (§10, §18).
    An entry that lands too late is dropped under the rule below. Its
    publication is settled, but its requested change has not succeeded.
  - Two online devices can still act concurrently: each can finish reading
    before the other's entry is stored. Replay decides their result.
  - Local app writes and local schema migration remain available offline.
    Publishing a schema raise needs storage, like publishing any entry.
  - E.g. Ana's phone cannot queue “remove Ben” while offline. Once it
    catches up online, the call either starts with that membership view
    or returns the reason the change is no longer allowed. If Ana is the
    sole admin, demoting her returns `Rejected(NoAdminLeft)`—the same reason
    replay gives that candidate in that view.
- Roles:
  - several equal admins;
  - only admins add and remove members, and change roles;
  - a member replaces their own keys, or an admin replaces another member's
    keys. Authority uses the current signing key and role in the entry's
    recorded view. Both signing and sealing keys must be fresh: neither may
    reuse a key established for any member earlier in that view or the replay;
  - each member adds and removes their own devices, with their own key, and
    admins can remove any device;
  - each member records their own new storage access; an admin may also record
    a replacement S3 access key for another member. The author must be that
    member or an admin in the entry's recorded view; the target must exist in
    that view and at its replay position. A later removal takes back every
    recorded access, including dropped entries
    ([§13](#13-removing-members-and-devices));
  - a removal names a device its author had read the addition of, so it
    knows whose device it is;
  - the store always has at least one admin.
- Every device that has the same entries ends with the same member list,
  whatever order they arrived in.
  - The machine-checked store-log model ([Appendix C](proofs/storelog.md))
    must establish this for these conflict and replay rules, including
    concurrent changes to different members, circle deletion, and the
    storage-time drop and finality rules below.
- A device applies an entry once it has every entry that entry had read.
- Each device keeps, in coven's local tables:
  - `_coven_store_log`: every entry it has applied, as downloaded and
    checked, its storage time, its immutable author-view checks, whether
    it landed too late, and whether replay kept or dropped it;
  - one optional finality horizon `H`: the greatest cutoff established by a
    qualifying storage-time window. No qualifying window means no horizon,
    not a zero timestamp. An entry is final exactly when its immutable
    storage time is strictly before `H`; there is no per-entry final flag;
  - `_coven_store_log_uploads`: locally authored entries with their numbers,
    timestamps, recorded past, plaintext and sealing key id fixed before upload;
    `_coven_key_uploads`: sealed objects whose `before_entry` names this entry,
    confirmed first.
    An entry receives its number when these rows commit. A pending entry is
    published before another is made, so numbering remains contiguous. Once
    stored, it is applied through the same replay boundary as a download, and
    that transaction deletes its queue rows. If publication or its reply fails,
    the next store-log step re-seals that plaintext with the recorded key,
    deriving its nonce as in §6, and sends identical bytes;
  - The same `_coven_key_uploads` holds historical copies with no
    `before_entry`. Before reserving a copy, choose recipients from the latest
    replay and skip a need already satisfied by an observed copy. Unmet sharing
    needs have no copy number yet. Once reserved, the object always retries
    its fixed bytes, even if another writer supplies the key or membership
    changes. Confirmation retires its queue row; cancellation cannot make
    a gap that hides later copies. Failure reaches the caller and remains visible;
    prerequisite copies must be confirmed before their entry is published;
  - the replay's result: `_coven_members` (every member a kept entry
    added, their stable ids, current public key pairs and role, and whether
    they were removed),
    `_coven_devices` (every device a kept entry added, its member and name,
    and whether it is active, removed or replaced),
    `_coven_circles` (every circle a kept entry made and its name),
    `_coven_circle_members`, and `_coven_circle_deletions` (each deleted
    circle's responsible kept entry, including when its creation dropped).
    The store's id and name come from its create-store entry; directory
    settings supply them while creation or bootstrap is incomplete and must
    match that entry when it is read. There is no duplicate store table.
    `_coven_boundaries` holds each checked raise/reset boundary;
    the current boundary per audience is derived from the kept entries.
    `_coven_drained_devices` separately records this device's completed
    retirement drains, with their kept-entry basis (§6). The local catalog
    retains the qualifying storage time establishing each retirement's
    finality until its drain completes; this times the additional wait,
    without replacing `H` as the finality test.
  - Ana's directory settings name “Recipes” while its first entry is being
    published. Once that entry is checked, reads derive “Recipes” from it;
    a conflicting setting fails opening, and preparing or applying that
    entry, with `DbError::StoreSettingsMismatch` rather than choosing
    another name.
  - Private member keys and audience key bytes are only ever in key custody
    (§11). Audience-key selection uses
    introductions in the received entries and the pass's new-copy observation
    (§11). There is no shared current-audience-key field or local retired-key table;
    permanent sealed copies are the shared record of who can hold each key.
  - Removed members and removed or replaced devices stay. Their stored
    writes still count when their authority and landing checks pass, using their keys
    ([§10](#10-device-identity)).
  - A causally ready batch of entries and its resulting replay commit in
    one transaction, so the tables always hold the replay of exactly the
    entries kept. Author-view checks still use each entry's own recorded
    past. The decoded store log stays with the sync owner between passes
    ([One sync pass](sync-pass.md#what-survives-a-pass)).
- Every store-log effect is computed from the entries received. An
  arriving entry can drop one kept before, or bring a dropped one back.
  - Keep the original inputs until every entry the effect depends on is
    final: rows, merge records, losses, file references and local sources,
    boundary snapshots and pre-migration data.
  - Applying a new replay replaces its derived database state atomically.
    If rebuilding fails, keep the previous committed state and report the
    failure; do not publish positions over the unfinished work.
  - Snapshot eligibility follows the received entries too. A snapshot's
    authorized key remains readable when membership replay changes (§11).
    Retention cannot erase inputs a later replay needs.
  - Local data changed by foreign-key actions follows the same rule (§8.4).
    Clearing a reset's error records is reversible until that reset is final.
  - App subscriptions report current results. Devices need not produce
    the same sequence of callbacks while they receive different entries.
  - E.g. Ben deletes Gifts, and Carol's device applies it. Then Ana's
    removal of Ben from the store arrives, made concurrently: a store
    removal beats a circle deletion, which is dropped. On Carol's device
    Gifts is back, with Ana in it, and its rows return, since the rule
    that took them out no longer holds ([§14.7](#147-deleting-a-circle)).
  - Entries waiting on ones they had read stay in storage until those
    arrive.
- An entry *lands* when storage publishes its complete object. Use that
  storage time, never its author's timestamp or a receiving device's clock.
  Thirty days means 30 × 24 hours.
  - An entry is *late* if it landed without having read every entry stored
    before it. Its recorded positions include its own earlier entries,
    and count both kept and dropped entries as read.
  - Drop an entry if it lands more than 30 days after the storage time of
    any entry it had not read. Exactly 30 days does not drop it.
  - Judge this against all stored entries, including dropped ones and
    entries from newly discovered devices, not only the author's past.
    Complete that check before replay or saving author-view checks.
  - This drop is permanent: keep the entry and pass its position, but
    exclude its change from every replay, including author views. It is
    not a damaged object and does not stop its device's log.
    Its keys and recorded storage access still follow §11 and §13.
  - Its author's device records `Dropped(LandedTooLate)` in the pending
    list: “landed too late”. A new attempt at the action catches up online
    and uses a new entry; it cannot rewrite the stored one (§18).
  - Online calls read the log first, but an upload racing another device
    or retrying after a connection failure can still land late. The rule
    applies to every store-log entry, including device registrations.
    App writes do not inherit this permanent entry rule. A retired device's
    writes have the replay-dependent landing deadline in §10; active
    devices' ordinary late writes follow §15.
- An entry is *final* once later arrivals cannot change whether replay
  keeps or drops it. At a finality observation time `T′`, defined below:
  - entries stored strictly before `T′ - 30 days` are final if no entry
    stored from `T′ - 30 days` through `T′` is late;
  - include both ends of that recent window, and include dropped entries
    when checking it. Entries with the same storage time stay on the
    same side of the window;
  - this test advances finality only after more than 30 days without a
    late entry. An entry once final stays final; later races cannot reopen it.
    The retained store log also lets a device establish finality from an
    earlier qualifying window. Recovered endpoints use the sample's provider
    ticks at resolution δ: the last endpoint before a late entry is its
    storage time minus δ, never an instant between provider ticks.
  - For each qualifying window ending at `T′`, atomically save
    `H = max(previous H, T′ - 30 days)` with the replay and changes to
    queries that depend on finality. With no previous H, use that cutoff.
    A failed or non-qualifying scan cannot advance H. Retain H through
    restart; a later late entry never lowers it.
  - Ana has established H = day 10. Ben's entry stored on day 9 is final;
    Carol's entry stored exactly on day 10 is not. A later qualifying
    window establishing H = day 11 makes Carol's entry final too.
- Establish the provider's stored time `T` before listing the store-log
  device folders. The finality observation is `T′ = T - M - δ`, with
  `M = one hour` for every provider and positive provider time unit δ (§4).
  The extra tick puts the inclusive endpoint strictly before §4's listing
  boundary `T - M`. If the subtraction cannot be represented,
  this sample cannot advance finality; never clamp it back to T.

  Then read each undrained writer from its next entry through a terminal miss,
  including newly discovered writers from entry 1. Permanent gap-free logs
  make these observations, together with retained entries, every entry
  at or before `T′`; completed drains supply the retained retired-writer
  history (§6). Each such writer's entry 1 was stored strictly before
  `T - M`, so §4 requires its folder to appear. Exact next-number reads
  supply the rest of its prefix. This also follows in the
  [IO proof](proofs/io/CovenIO/ListingFinality.lean).
  Newer entries may be absent and do not enter this certified window.
  Unreadable entries newer than `T′` remain pending outside this certificate;
  they still prevent a complete catch-up.
  Received newer entries still use their actual recorded reads for lateness;
  a writer whose listing missed another entry can genuinely be late.
  Read and judge the complete prefix through T′ before advancing finality.
  A gap, unreadable entry or failed folder listing
  blocks that check and the cleanup that needs it (§19.1).
  - A provider-assigned stored time already observed is a lower bound on
    storage's current time. Reuse times from the pass's listings and
    successful publications, but only a time known before this store-log
    scan can supply its T. Subtract both M and its provider time unit before
    using it for finality; a later observation serves a later scan.
  - Schedule a fresh time observation only when recorded storage times
    indicate that finality, a retirement drain (§6), or retention could
    cross its next threshold.
    The device's timer schedules the check; it never proves storage age.
    An observation still short of the threshold backs off before retrying.
  - For a fresh time, replace `<store>/clock/<device>` with a sealed,
    signed clock object, then read its complete publication time with status.
    This device is its only writer. Keep one object, replacing it only when
    time-dependent work is due; positions are never reposted for time.
    The identity check precedes the replacement. The folder listing and
    next-number reads that use this sample start after the status response.
    A failed replacement or status call supplies no new T or T′ and remains
    visible as pending work.
  - T remains the storage-age sample for retention (§15), the return check,
    and the qualifying time saved as F when a retirement becomes final (§6).
    Only the finality window and its horizon use T′.
  - Ana's covered log waits for its thirtieth storage day. Her timer wakes
    sync, which replaces her clock object and reads its status: two logical
    requests, plus any provider lookup or confirmation calls. If storage
    still says day 29, she keeps the log and schedules another check.
  - **Ben's first entry is exactly one hour old.** Ana's S3 clock sample
    is 12:00:10. Ben's new phone stored entry 1 at 11:00:10; a listing at
    that sample's tick may still omit it. Ana's inclusive endpoint is
    11:00:09. An entry stored at 11:00:09 must be discovered and read before
    that window can qualify. At the next tick, Ben's entry must appear too.
  - No device's acknowledgement is required. A sleeping device or a
    concurrently registered one has the same landing deadline as any other.
- Why this holds:
  - every entry in the recent window read all the older entries;
  - every future entry that escapes the time-based drop must also read
    all those older entries;
  - those reads force later timestamps (§7.2) and prevent concurrency with
    the older entries. Replay therefore keeps the same older prefix and
    cannot change its kept or dropped results, even through a chain.
- E.g. Ana and Carol are admins; Ana and Ben share Gifts. Ben's fast clock
  makes the timestamp order A, C, B:
  - Day 0: Ana's phone stores reset A. Ben's laptop has already attempted
    deletion B without reading A; its upload has not landed.
  - Day 28: Carol's tablet reads A and attempts removal C of Ben from the
    store. C has a later stamp than A but an earlier one than B.
  - Day 29: B lands. It missed A by 29 days, so it survives and defeats A.
    B is late, so A cannot become final on day 30.
  - Day 31: C lands. It read A and missed B by only two days, so it
    survives. C defeats B before B can defeat A; replay keeps A and C.
    C is late too, so the wait starts again.
  - Once T′ passes day 61, with no further late entry, all three results
    are final: the provider sample T must pass day 61 plus one hour plus δ.
    Cleanup can release the inputs no longer needed by the winning reset.
    If C had instead landed after day 59, it would have missed B by more
    than 30 days and been dropped as “landed too late”.
- Only physical cleanup waits: logs covered by snapshots, a deleted
  circle's rows, files and objects, pre-breaking-change inputs, and local
  sources kept for reversible effects. Release them once every entry they
  depend on is final and their other retention checks pass (§15, §16.5).
  Key rotation and provider revocation act on the current replay immediately.
- The member list is what you get by replaying the applied entries in
  timestamp order, from the first.
  - For each causally ready arrival batch, the device replays the resulting
    received set once, from the first, so the result depends only on which
    entries it has. It does not replay the same growing set separately for
    every entry downloaded in that batch.
  - The member list an entry's author had read is the replay of just the
    entries that entry had read.
  - That past never changes once the entry is applied. The device keeps
    the checks derived from it with the entry: authority, a removed device's
    observed owner, and the circles a member removal deletes in that view. These commit in
    the same transaction as the entry and are reused on later replays.
    Replay marks start afresh for each ready batch; the permanent
    “landed too late” exclusions remain.
- At its place in the replay, an entry applies only if its author's role
  allowed it, in the member list the author had read.
  An old device's entry must also precede its reading a kept replacement,
  judged from its recorded store-log past (§10).
- Then, an entry whose change is already in place applies and changes
  nothing.
  - E.g. Ana and Ben both add the same Dan id, keys, role and access: both
    apply, Dan is added once, and neither is dropped. An addition cannot
    change an active member's keys; use Replace member keys instead.
  - Replace member keys never takes this shortcut. Concurrent replacements
    of the same id conflict even when their key pairs are identical.
- Otherwise it applies only if:
  - what it needs exists: the member it changes, the device it removes,
    the member a new device belongs to, the circle it changes, the circle
    member it removes;
  - the store still has an admin after it;
  - it beats every concurrent entry already applied that it conflicts
    with.
- When an entry beats some already applied, they are all dropped, and the
  replay starts again without them.
  - A replay-dropped entry stays dropped until that replay ends; the next
    ready batch starts afresh, still excluding entries that landed too late.
- A dropped entry has a pending record with its reason. Replay reasons
  can change until finality; “landed too late” cannot. Neither is a
  permanent refusal of the entry's bytes (§19.1).
- Whose entry it is comes from its signature, not from the device it was
  written from, so a new device adds itself, signed with its member's key
  ([§12.1](#121-a-persons-new-device)).
- Concurrent entries conflict only when they contradict:
  - different states for the same member or device, such as granting and
    removing the same membership, or assigning different roles;
  - one removes a member or device the other's change requires to exist;
  - one deletes a circle the other changes, adds to, removes from, resets
    or raises. A rotation changes no membership and conflicts with nothing;
  - two raises to the same audience and version with different snapshots,
    two resets of one audience with different snapshots, or a reset and
    raise of that same audience.
- Sharing a member id or rotating audience keys is not itself a conflict.
  - Ben's two device additions, his new storage access and his circle
    addition can all apply.
  - Adding Carol and removing Dan both apply. Carol receives every key
    she needs; keys that reached Dan are retired for new sealing (§11).
  - Adding Carol to Gifts and removing Ben from Gifts both apply too.
  - A member-key replacement conflicts with another replacement of that id
    or removal of that member; it creates no conflict with role, access,
    device or circle changes. Replacements of different members do not conflict.
  - Two removals of different members both apply if an admin remains.
    Two removals of the same member are the same membership result.
  - Renames use their existing latest-timestamp rule (§14.3), and raises
    to different versions keep the higher version (§17.1).
- For conflict comparison, an entry deletes a circle when it explicitly
  deletes it or removes its only member in the author's recorded view.
  A store removal also deletes the circles its author saw with only that
  member. This classification does not change on later replay.
- Deletion conflicts with changes requiring the circle because they cannot
  both take effect on an existing circle. The deletion wins by the tiers
  below; sharing or rotating an audience key is not itself a conflict.
- Of two conflicting entries, the one that beats the other is:
  1. removing a member or device from the store;
  2. deleting a circle, including removing its last member in the author's view;
  3. removing someone from a circle;
  4. other changes, except making someone an admin;
  5. making someone an admin.
  - A lower-numbered tier wins; within a tier, the smaller timestamp wins.
  - These tiers compare conflicting entries only.
- For concurrent replacements of the same member's keys, another admin
  beats self-replacement; otherwise the earlier timestamp wins. Classify by
  stable ids: author equal to target is self-replacement even when that
  member is an admin. Removing the target member beats every replacement.
  The target must exist both in the recorded view and at its replay position;
  replacement cannot restore a removed member. An equal-payload winner may
  retain the pair already installed by its defeated contender; this is the
  same replacement, not permission to reuse a historical key.
- Replace member keys carries only the target id and the new public pair.
  It preserves role, storage access, circle memberships and every device id.
  Audience keys rotate separately when their copies expose them (§11).
- An old-key write's window becomes final with a kept replacement and the
  final entry prefix through its storage time, including ties (§10).
  Before then, replacement decisions and dependent rows, source verdicts
  and losses change in the same replay transaction. The checked member-key
  proofs assume this final prefix; the general horizon theorem has not been
  extended to keyed replay ([C13](proofs/storelog.md#c13-stable-members-and-replaceable-keys--checked-proposal)).
- Concurrent entries, and what applies:

  ```
  Ana adds Dan               Ben makes Carol an admin   both
  Ben adds his phone         Ben adds his laptop        both
  Ben adds his phone         Ben changes his S3 key     both
  Ben adds his phone         Ana removes Ben            removal
  Ana makes Ben an admin     Carol makes him a member   member
  Ana adds Carol             Ben removes Dan            both
  Ana removes Ben            Ben removes Ana            earlier, if only two admins
  ```

- E.g. Ana, Ben and Carol are the only admins. Each reads the store log,
  then removes another before receiving the others' entries:

  ```
  first stamp    Ana removes Ben
  second stamp   Ben removes Carol
  third stamp    Carol removes Ana
  ```

  - The first two apply. Ben's authority is checked in the view he read,
    not the member list left by Ana's earlier concurrent removal.
  - Carol's removal would leave no admin, so it drops. Ana remains admin.
  - Ben and Carol stop when their devices read their own removal; neither
    can start another removal after that.
- With only Ana and Ben as admins, their concurrent removals of each other
  leave the earlier remover as the remaining admin. The other removal drops
  because it would leave none.
  - A device that already observed its own removal stops for good (§10).
    It does not keep reading to discover a later reversal. In this case,
    another device of the remaining member may need to add it again.
- A replay-dropped entry stays dropped for that replay even if its defeater
  later drops. The next ready batch starts a fresh replay, with time-based drops
  still excluded.

## 10. Device identity

- A device is one install of the app, with its own device id, belonging to
  one member, who adds it to the store log ([§9](#9-members-and-roles)).
- Before any upload, check that this installation still owns its counters.
  - Use the pass's next-number reads of its own writes, store-log entries
    and key copies, plus checked own posted positions and signed snapshot
    coverage. On reopening or reconnecting, inspect the snapshot folders
    needed for own-counter evidence before sending; writes or snapshots
    from a rolled-back database may already have been deleted. Retain that
    evidence for the running session. This is initialization work, not an
    every-pass history listing; all occupied paths still require comparison.
  - A stored number beyond this database's last reserved number for that
    kind of object proves this is a stale copy. An outstanding local reservation is not stale
    merely because its upload succeeded before a crash.
  - The device id also lives in custody that backups do not copy. A missing
    or different custody id requires the same device reset.
  - Finish this check before sending writes, entries, snapshots, files,
    key copies, positions or clock observations, including sends outside
    the periodic sync loop.
    A failed check sends nothing and reports why it is pending.
  - Check the store log for this id's replacement before sending too.
- Every sender uses the last completed membership and key-copy catch-up
  (the pass's first phase). Before each send, its start must be less than
  five minutes ago on a monotonic clock that includes sleep. Measure from
  the start, never the completion. An incomplete catch-up cannot refresh it.
  - The pass, file-upload worker and calls outside the loop share this gate.
    It covers first attempts, identical retries, upload-session requests,
    parts and completion, as well as entries, key copies, snapshots,
    positions and clock objects. Recheck immediately before starting each
    provider request, including an SDK retry; a queued request has no exemption.
  - At five minutes, or with no completed catch-up, wait for the next pass.
    Per send this is a clock comparison; membership and copy IO stays with
    the pass. Reopen, reconnect or device reset invalidates the session's
    freshness evidence. The identity and removal checks above still apply.
  - A clock-object replacement with no fresh catch-up first needs a read-only
    first phase, including the identity evidence. Its time can qualify only
    a subsequent store-log observation (§9), not the one that enabled it.
  - Assume each provider request completes or fails within one day
    (24 hours), including any publication after a lost reply: it cannot
    publish later. A multipart session can last longer, but every subsequent
    request needs the gate again. This is a provider-duration assumption,
    not a claim that a client timeout cancels remote work.
  - Each request also requires the sending installation's signing credential
    to be current in that completed catch-up. For a signed object its signer
    must be current too, including a snapshot, positions or clock retry.
    A write or entry retry must still use its fixed signing key; current
    custody cannot re-sign an old object or change its recorded past to pass
    this gate. Creation authenticates its supplied initial pair (§9), and a
    join request authenticates its invite-bound candidate pair (§12). These
    bootstrap exceptions grant no ordinary sending authority before a kept
    binding; registering a new device requires that binding's current key.
  - Once a retiring entry and its preceding prefix are final (§9), a
    catch-up started after finality observes the retirement and refuses the
    old device id or signing credential. Earlier catch-ups permit sends for
    less than five further minutes, and each request settles within one day.
    No such publication occurs at or after finality plus one day and five
    minutes. Measure from finality, not the entry's publication: replay can
    drop and restore the retirement before it is final. Key replacement
    bounds old-credential publication; it does not end the member's device
    logs. Devices using the new credentials continue those logs.
- A stale copy resets as a new device using storage and saved custody.
  - Stop its local writes and transfers. Discard its database and pending
    local work; do not salvage or renumber waiting writes.
  - Use the existing new-device installation path (§12.1), with a fresh id.
    Load the stored history, then allow app writes again.
  - Tell the app which old id was replaced and why, and that unsent edits
    were discarded. Store the `DeviceReset` record in `_coven_device_reset`
    in the replacement database before publishing its usable handle (E5).
    Bootstrap retains the old/new ids and reason until that record commits;
    directory publication and returning a usable handle require that commit.
    The record survives reopening, is local to this installation and is not
    snapshot data. `device_reset()` and `subscribe_device_reset()` use the
    same query.
  - Until replacement finishes, keep the installation unavailable. Retry
    through the existing bootstrap state, using the same new id and entry.
    Never reopen the discarded copy as a working store.
- The new device's add entry names the old id it replaces (D6).
  - The old id is shown as replaced. This changes no membership or keys.
  - Only that member can replace the id. The new id differs from the old.
  - Concurrent replacements both apply. Each new device keeps its own id;
    the old id remains replaced, with no recorded last write or entry.
  - Judge an old-id write by its `store_log_read`, and an old-id entry by
    its `had_read` including its implicit earlier own entries. It counts
    only if that past contains no kept replacement of the old id, just as
    an object made before reading removal can count. A write must also land
    within the retirement deadline below; entries keep §9's permanent
    landing rule. Ordinary authority, causality, schema and reset rules apply.
  - Publication after replacement does not change what the object had read,
    but its storage time decides the landing check. There is no upper number
    to extend and no wait for another replacement to include it. Retain
    relevant inputs until the deciding entries are final; a changed replay
    recomputes write admission atomically (§9).
  - A still-running old copy stops sending when it reads its replacement,
    and resets the same way, choosing its own fresh id. It does not take
    over the id of the device whose replacement it read.
  - Replacement does not exempt an entry from §9's landing rule. A stale
    registration or replacement retry that lands too late is consumed and
    reported; registering again uses a new entry after catching up online.
  - Other devices keep polling the old id until a kept replacement has been
    final for more than one day and five minutes, then drain its three logs
    once and record completion locally (§6).
- E.g. Ana backs up her phone after write 5, then uploads writes 6 and 7.
  Her restored phone still has counter 5.
  - Its storage check finds newer writes before it sends anything. It registers
    as `ana-phone-2`, replacing `ana-phone`, and loads 6 and 7.
  - Its first new write is `<store>/devices/ana-phone-2/1`.
    Edits made only in the restored copy are discarded, with an app notice.
- Ana's still-running old phone can have write 8 in flight when that
  replacement lands. If write 8 was made before the old phone read the
  replacement and lands no more than 30 storage days after it, readers
  count it under the ordinary write rules. It needs no second replacement.
  A landing after that deadline is excluded while the replacement is kept.
  Reading the replacement stops the old phone before further work or retries.
- A restored copy that is behind storage never sends. A backup restored
  on the same installation with custody intact, nothing newer in storage,
  and no removal or replacement continues as that device. The checks cannot
  detect every restore; content-bound nonces still separate different
  plaintexts even if a backup forgot an encryption attempt (§11.1).
  - Ben backs up one queued write before attempting it. Restoring that
    backup on his laptop leaves its custody and stored counters unchanged.
    The check passes and that same device uploads the queued write.
- An occupied immutable path succeeds only if its complete stored bytes
  equal the bytes this attempt would send. Compare before retiring its queue
  row; a mismatch on this device's path triggers the same device reset.
  - This applies to writes, entries, snapshots, files and each writer's
    sealed key copies. Different writers use different key-copy paths.
  - A read failure keeps the attempt pending; occupation alone proves nothing.
  - Two live copies can pass the check together and send different bytes
    to one path. Content-bound nonces separate their encryption (§11.1);
    the byte check still resets the losing copy and discards its unsent edits.
- Every write record is signed with the current signing key of its device's
  stable owner in the replay of its `store_log_read`. The device path and
  that recorded view supply the author and key; the write adds neither field.
- This is about authenticity, not trust.
- A write counts only if its author was a member, and its device one of
  theirs, in the store log the write had read ([§7.1](#71-causality)), so
  a write by Ana's phone counts as Ana's.
- For every kept entry retiring a write's device, the write counts only if
  its `store_log_read` does not include that entry and storage published the
  complete write no more than 30 days after the entry's storage time.
  Device retirement is removal of the device, removal of its member, or
  replacement of its id. Exactly 30 days is allowed. Ordinary authority, causality,
  schema and reset checks still apply.
  - Use first complete publication times, never authored timestamps or
    device clocks. Retries change neither bytes nor storage time (§4).
  - A write made after reading its own kept retirement is refused before
    its position passes (§7.1). A pre-retirement write published outside its
    window is instead dropped as `Dropped(LandedTooLate)` for its write
    subject. Consume its position without effects or losses; this is not a
    permanent refusal and does not stop the log. Retain its original inputs
    and storage facts while a replay could restore it.
  - Judge each write by its own publication window. Later readers cannot
    rescue an outside-window write, whether their own changes apply or lose.
    Otherwise eligible changes built on its dropped row changes become
    recorded losses under §8; their write's unrelated changes still apply.
  - Recompute this verdict with replay and rebuild affected rows, source
    verdicts and losses in the same transaction (§9), including contributions
    already in snapshots. If the retiring entry drops, this exclusion
    disappears; another kept retirement can still exclude it. A failed
    rebuild leaves the prior state intact and reports the failure.
  - With the send gate above, this retirement verdict is final once its
    deciding kept retiring entry is final (§9). Its preceding store-log
    prefix, including storage-time ties, is then stable. Later retirements
    cannot reverse either window verdict: an admitted write already met the
    final entry's earlier deadline. This needs no future reader or drained
    reader log. Schema and reset still decide their own independent verdicts.
    The one-day-and-five-minute wait bounds discovery, not admission finality.
    [B17 proves both results](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data).
  - Entries instead keep §9's permanent rule: more than 30 days after any
    unread entry excludes them, whether that entry is kept or dropped.
    Dropping a retirement can restore a write, never an entry that landed
    too late. Neither landing rule relies on §10's request-duration assumption.
- E.g. Ana's removed phone publishes a note one second outside its window.
  The removal was temporarily dropped, so Ben could read and edit that note
  before replay restores the removal. Ben's same write renames a playlist
  whose generation and replaced setter survive.
  - Ana's late note is dropped on every device. Ben's note edit is a frozen
    loss naming its dropped sources; his playlist rename applies.
  - The result is the same whether Ben's edit arrives before or after a
    concurrent delete of the note. That dependent delete also has a loss
    record, even if it carries no scalar values.
- A device that reads its own removal, or its member's, stops syncing for
  good and tells the app ([E5](api.md#e5-storage-and-sync)).
- Replacing member keys preserves the member and all device ids. A device
  with the old credentials stops sending and reports `MemberKeysReplaced`;
  it cannot publish queued old-key objects under the new key. Restoring with
  the current code installs a new device (§12.1); it does not renumber or
  rescue that old queue. A self-replacement first pauses local app writes
  and confirms its queued writes and entries, then publishes the replacement.
- A write signed with replaced keys must first pass the recorded-view
  authentication above. For every unread kept Replace member keys entry
  targeting its owner and installing a different signing key, the write
  counts only if its first complete publication is no later than the entry's
  storage time plus 30 days. Equality counts; an earlier publication already
  meets that deadline. Check every such entry, including when the write used
  keys from a losing replacement. A replacement already read does not retire
  a later key current in that recorded view.
  - An admitted reader never rescues an excluded write or entry, including
    through implicit earlier own writes. The old-key write is consumed
    without effects or losses; its author sees `Dropped(LandedTooLate)`.
    An otherwise admitted row change using its data instead becomes a
    per-change loss under `DataDependency.dependsOnDropped` (§8). Unrelated
    changes in the same write remain eligible.
  - Entry rejection stays permanent under §9's comparison with every unread
    stored entry, kept or dropped. A write's read cannot rescue a late entry.
  - Old-key window admission is final when one kept retiring replacement and
    the complete preceding entry prefix through its storage time are final,
    including tied storage times. Later replacements cannot reverse that
    window verdict; schema, reset and other admission rules remain separate.
    Until then, preserve original inputs and rebuild admission, source
    verdicts, rows and losses atomically on a replacement decision change.
  - This grace period admits qualifying old signatures. It is neither
    immediate signature revocation nor a theft-recovery guarantee; provider
    cut-off remains separate (§13).
- Removing a device takes away its storage access, so nothing it writes
  after confirmed cut-off can reach other devices. Until then, readers
  enforce the admission and landing rules above (§13).

## 11. Keys

- Coven uses four kinds of key:
  - the store key, which encrypts what coven writes to storage, except a
    circle's rows and files and the sealed keys;
  - each circle's key, which encrypts that circle's rows and files
    ([§14.3](#143-circles));
  - each member's current signing and sealing keys, bound to their stable
    `MemberId` (§9), which sign objects and open store and circle key copies;
  - storage credentials: each device's own sign-in to the provider, or on
    S3 its member's access key ([§4](#4-storage-providers-and-access)).
- Each store or circle key has a random id, picked when it is made, and the store log entry
  that brings it in names it ([§9](#9-members-and-roles)):
  - the store's first key, the entry creating the store; a circle's first
    key, the entry making the circle;
  - each later key, a rotation entry naming its audience. Removals carry
    no keys. After audience creation, rotation is the only way a new store
    or circle key is made.
- Every key-introducing entry—create store, create circle or rotate key—also
  carries `key_hash`, SHA-256 of the exact 32 key bytes, inside the encrypted
  entry. After opening a copy, check its audience, id and hash against that
  authorized introduction before accepting the key for ordinary reads or
  sending. A mismatch is an invalid copy; the recipient ignores its key.
  - A candidate needed to open its introducing entry is tentative until that
    entry's signature, authority, identity and hash checks succeed. It may
    open that entry for validation, but cannot enter the accepted key set
    or authorize other data before those checks.
  - A publisher validates its locally authored introduction against the
    caught-up view before reserving it. Its fresh key may seal that introduction
    itself; other first attempts wait until the entry is stored and kept.
    This applies to creation and rotation and does not authorize unrelated
    data with an unpublished key.
  - Dan plants chosen bytes under K2's copy path for Ana. Ana opens the box
    but rejects its hash; those bytes cannot become K2 in her custody.
- A copy addressed to an excluded member, a retired sealing credential,
  or a credential that is not the named member's current one counts as
  exposure of the named audience key. A device cannot open a box sealed to someone else, so it cannot
  check that box's key hash. Exposure detection uses its named audience,
  key id, stable recipient id and exact sealing key; it does not require
  proof that the recipient opened it.
  - Dan is excluded. Someone publishes a forged K2 copy addressed to Dan.
    Ana cannot open it, even though she holds K2, and retires K2 for first
    attempts. That forged copy costs at worst an extra rotation; it cannot
    make Ana accept a different key under K2's id.
  - Recipients still ignore copies whose opened key fails the introduction's
    hash. That check governs key acceptance, not third-party exposure.
    Retain the invalid-copy verdict and continue to the writer's next copy;
    a later valid copy can supply the key.
- Each store key is sealed to every member's current non-retired sealing
  key. Store and circle
  copies share the writer's log at `<store>/keys/<writer>/<n>`, numbered
  from 1 with no publication gaps. Copies are never deleted.
  - The clear prefix holds the public routing facts: audience, key id and
    recipient id and exact sealing public key (D9/D10). The authenticated
    prefix's sealing key must equal the key actually used by the box.
    Key bytes and their hash remain encrypted. Each copy
    is a small object, read whole by its discovery GET. Keep its routing
    facts and bytes.
  - Numbering belongs to the writer, across all audiences and recipients.
    Reserve fixed bytes with the number before sending, and settle that
    number before publishing the next. An obsolete reserved copy cannot
    be dropped to make a gap; §11's exposure rule accounts for its recipient.
- Concurrent removals change membership independently. Once both arrive,
  keys known to have reached either excluded member are retired for first
  attempts. A remaining member uses an existing usable replacement or makes
  one by rotation. Removal never makes a key on behalf of a circle outsider.
- Historical keys remain readable when replay changes membership. Every
  device holding one shares it with current audience members lacking it.
  If no reachable device holds a needed key, its objects remain key waits.
  - Ana removes Ben; Carol rotates to K2. If a concurrent replay returns
    Ben, Carol supplies him K2 so he can read objects already sealed with it.
- Several keys for one audience may coexist. Each object names the one
  that sealed it; arrival of another key does not invalidate old objects.
- Ana's phone and Ben's laptop can both seal K to Carol. They publish
  `keys/ana-phone/17` and `keys/ben-laptop/6`, each naming K and Carol;
  neither replaces the other's randomized sealed bytes.
- Each sync pass GETs every undrained writer's new copies through a next-number
  miss, alongside the store log. Complete both observations before selecting
  keys for first attempts. A failed or incomplete read blocks those attempts
  and records the failure (§19.1); an older miss cannot stand in for it.
  A completed retirement drain (§6) supplies that writer's complete retained
  copy history without another probe.
  - An observed copy means its recipient may hold the key, whether or not the
    recipient has downloaded it. Copies remain in storage for good.
  - A key with an exposed copy under the rule above is retired for first
    attempts. Keep it for reading and for
    identical attempted retries. An addition or removal that drops does not
    erase its copies; an introducing entry returning does not erase them either.
  - Retirement is derived from all observed copies and replay, not remembered in
    a separate local table. A recipient returning to the audience is no
    longer excluded, but copies to its retired or noncurrent credentials
    still retire the key.
  - If no usable replacement is held, a current audience member's device
    acquires one or makes a fresh key, seals it to current members, then
    publishes a rotation entry. Old exposed copies remain known forever;
    their presence does not cause another rotation once a usable replacement
    is held. Only a current circle member makes its replacement key (§13).
  - Several devices may rotate at once. Their keys have distinct ids;
    both remain readable. A rotation conflicts with no entry; its author
    must belong to its audience in its recorded past (D6).
- For a first attempt in an audience this member still belongs to, use
  the newest usable key this device holds, ordered by its introducing
  entry's timestamp, then key id.
  - Usable means introduced by a received authorized entry, available in
    custody, and with no observed exposed copy after this
    pass has read each undrained writer's new copies through its terminal
    miss, together with the complete histories of drained writers.
  - If no usable key exists, rotate or wait for its sealed copy, recording
    the first unmet condition. Never use a known exposed key as a fallback.
  - This selection governs app data: writes and snapshots. Store-log entries,
    positions and clock objects carry membership and sync metadata, not app
    data; they are sealed with the newest store key this device holds and
    accepts. With an accepted store key in custody, exposure cannot block
    sealing a removal or rotation entry. Until storage revocation is confirmed
    (§13), a removed member can still obtain and open metadata sealed with a
    key they retain or later acquire. Cutoff prevents new downloads, not opening
    ciphertext already obtained
    ([sealing and read conditions](proofs/storelog.md#store-log-entries-positions-and-clock-objects)).
- Every kept Replace member keys entry exposes the preceding sealing
  credential from its recorded view, regardless of why keys changed. Copies
  to that credential retire their audience keys even though the member stays
  active. Only current members of each affected audience may rotate its key.
  A store admin outside a circle cannot rotate for that circle.
  - Catch up before reserving new copies. Ordinary historical sharing and
    rotation route to the current non-retired sealing key. Binding-entry
    prerequisites instead route to the validated candidate's fresh keys (§12).
  - A candidate's copies remain if its replacement loses or never becomes
    current. Those copies expose the audience key too, even for an active id.
    Sharing an exposed historical key with the winning new credential does
    not make that audience key safe for first attempts again.
- Share every historical key with current members who lack a copy for their
  current non-retired sealing credential. A membership
  reversal does not revoke an authorized key introduction. Sharing adds a permanent sealed copy;
  every device's next-number reads can observe it.
- The guarantee uses the sending device's replay and copy observation at the start
  of this pass. Every send requires that completed catch-up to have started
  less than five minutes ago (§10); there is no per-write or per-upload
  membership or key-copy IO. First attempts use that pass's selection;
  tried writes keep their bytes and obey the same freshness gate.
  - E.g. Ben's tablet sees Ana's removal drop and shares key K with her.
    Ben's phone sees the removal kept throughout. Its next reads of the
    tablet's copy log find K's copy for Ana: it retires K and rotates before
    first sending another store write.
  - A copy published after the phone's miss for that writer is the residual
    window. The phone may first-send with K during that pass while Ana can
    obtain the new copy.
    Its next pass retires K if Ana is still excluded. This is accepted;
    the freshness gate bounds stale sending but does not close this window.
    Under §10's one-day request-duration assumption, even a retired writer's
    in-flight copy lands within one day and five minutes after its kept
    retirement becomes final (§9), even if replay dropped it earlier.
    Copy exposure cannot be undone by rejecting a late object.
  - Writes first sent before the sender learned of a removal also remain
    readable under their old keys while the recipient has storage access.
    Revocation ends that access; on S3 coven deletes the access key with
    operation-scoped admin access, or an admin deletes it in the console (§13).
- Every object encrypted with a store or circle key names that key outside
  its encryption, so a reader knows which key opens it.
- A file's independent key is carried in its row's encrypted writes
  ([§16.1](#161-kinds-and-where-files-are)).
- A device opens each copy with the historical private sealing credential
  named by its authenticated prefix, never by guessing from the latest
  member binding. Retain needed private credentials and all public bindings;
  a fresh installation can instead receive historical audience keys sealed
  to its new credential. Old copies remain immutable in the same writer log.
- Removal retires keys exposed to that member for first attempts. A remaining
  member's device publishes a rotation before using a replacement key.
  - First attempts made after learning the removal use a key eligible under
    the pass's copy observation, subject to the residual window above.
  - Devices keep the old keys, to read writes made before.
- Each device keeps its stable member id and private key custody in the OS
  keychain, including retained credentials needed for historical copies.
  Keep a self-replacement's candidate durably before publication; after a
  failed publication retain it and the previous pair for idempotent retry.
  Select signing authority from checked replay, not the last pair saved.
  Activating new credentials and refreshing restore-code custody must finish
  before reporting success or resuming app writes; failures reach the caller.
- Storage access, not keys, is what keeps a removed device out.
- The app can keep each of its own secrets, such as an API token, in a separate
  keychain entry, under the same access policy as coven's keys and with the
  platform's per-secret size limit. Names are arbitrary strings, encoded into
  native account names without colliding with coven's entries. Coven records
  each store's saved names in another keychain entry, outside the database.
  Saving adds the name before writing the secret; deleting removes the secret
  before the name. The list is always a superset of the secrets that exist:
  failure or a crash between steps can leave only a harmless extra name.
  Deleting the local store removes every listed secret, then the list and
  coven's other entries, even when the database is damaged, without the app
  supplying names. Removing an absent entry succeeds, so retrying after any
  failure is safe ([E1](api.md#e1-opening), [E11](api.md#e11-keys-and-secrets)).

### 11.1 Cryptography

- A member's keys are two key pairs, kept together:
  - an Ed25519 pair, which signs;
  - an X25519 pair, which opens keys sealed to the member.
- `MemberId` is an independent random UUID. `SigningKey` is a 32-byte
  Ed25519 public key with invalid and weak points refused; `SealingKey` is a
  32-byte X25519 public key. The public `MemberKeys` pair in D2 contains both.
  Replacing it requires fresh keys of both kinds (§9); it never changes the id.
- Everything coven writes to storage is encrypted with XChaCha20-Poly1305:
  write records, store log entries, snapshots, sealed keys and file
  chunks.
  - Each object's storage path is bound into its authentication, so the
    provider can't swap one object for another.
  - Each chunk of a file binds its index; each chunk of a write or a
    snapshot binds its section and its index: a write's header is
    section 0 and its part i is section i + 1, and a snapshot is one
    section.
- A write's signature covers its path and a SHA-256 hash of every byte
  before it, so a device checks it as the object streams in.
- Nonces:
  - for a file's chunks, the chunk's index, which never repeats under the
    file's own key, so retrying an upload sends the same bytes;
  - for writes and store log entries, the first 24 bytes of HMAC-SHA256
    under the encryption key, over a context binding the object's path,
    cleartext prefix, section, chunk index and SHA-256 of that chunk's
    plaintext ([D11](format.md#d11-keys-contexts-and-fingerprints)). Binding
    the prefix also separates changed authentication data. Rollback and
    copied identities need no unique-path assumption for this separation;
    it still relies on SHA-256 and the truncated HMAC resisting collisions;
  - for everything else, random.
- A key is sealed to a member with an anonymous sealed box: X25519 with
  XChaCha20-Poly1305.
- Keys for each purpose are derived from the store key, or a circle's key,
  with HKDF-SHA256 and a label per purpose: encryption and fingerprints
  ([§19.1](#191-noticing)).
- A join request's key is derived from its invite secret
  ([§12.2](#122-adding-a-person)) the same way, with its own label.
- An uploaded file's storage name is a random id, and it is encrypted with
  a random key of its own ([§16.2](#162-storage-and-naming)).

## 12. Joining and restore

- Restore and join take the app's configured, layout-scoped builder and return
  the open store ([E10](api.md#e10-joining-and-restore)). The app supplies its
  opening choices once; session-only key custody stays with that handle.
- An unfinished installation is hidden from listings and refused by ordinary
  opens. It stays at its permanent path while loading. Only after loading does
  coven commit final keys and credentials and publish it, without closing or
  moving the database. Cancellation closes it and removes its local work;
  dropping a waiting join preserves its encrypted request for an explicit retry.
- Dropping a bootstrap marks its installation as closing. Its locks remain held
  until its in-flight blocking work finishes. A bootstrap or open of the same
  store in this process waits for their release ([E1](api.md#e1-opening)); a
  still-live instance or another process's lock returns `AlreadyOpen` at once.

### 12.1 A person's new device

- A person's new device needs their *restore code*, which holds:
  - their stable member id and current private signing/sealing pair, which
    gets it the store key ([§11](#11-keys));
  - the store's id and name;
  - the storage location only, never storage credentials.
- On S3, the person keeps their access key separately, such as in a
  password manager, and enters it on the new device. Google Drive, Dropbox
  and OneDrive likewise need a sign-in to the person's provider account.
- It gets the code in one of three ways:
  - by scanning it as a QR code on one of the person's devices that has
    the store open. This device-to-device transfer may also carry the S3
    key alongside the code, because the person controls the showing device;
  - on Apple platforms, from iCloud Keychain, which holds the code and may
    hold the S3 key separately alongside it. A new device on the same Apple
    account still needs a provider sign-in where required, or the separately
    kept S3 key if Keychain does not hold it;
  - by the person typing it in.
- The QR code is blurred until the person taps to show it.
- Where storage needs a sign-in, such as Google Drive, the new device
  asks coven to sign in to the person's own account first, through the builder's
  presenter. Restore and join use coven's held tokens. Restore codes never
  carry OAuth tokens.
- The new device catches up and requires the code's id and public pair to
  match an active member's current binding. An old code cannot bind its keys
  back to that id: report `MemberKeysReplaced` and require current keys or
  a new admin-approved replacement invite. It then adds itself to the store
  log, signing with the current member key (§9), and follows the same landing
  rule as existing devices; finality does not depend on knowing it in advance.
- The person writes the code down when they create or join a store, as
  part of setup.
- On Apple platforms, coven writes the code to iCloud Keychain whenever it
  changes, including when the person creates or joins a store or replaces
  member keys. Member-key replacement refreshes private custody and the
  written restore code; the app asks the person to keep the new code. Changing an
  S3 key updates the separately held credentials; it does not change the
  restore code or require the person to write it down again
  ([E9](api.md#e9-members-and-devices)).

### 12.2 Adding a person

- On a provider that shares with an account, the admin who invites is
  the member whose account holds the store
  ([§4](#4-storage-providers-and-access)).
- An admin adds a person with an *invite*, a code shown as a QR code,
  holding:
  - the store's id, name and location;
  - the invite's id, the chosen stable member id, a one-time *invite secret*, the initial store key id
    from the creation entry, the inviting writer's device id, and that
    writer's last published key-copy number when making the invite;
  - on S3, an access key made for the new person using the admin-access or
    guided console path in §4.
- The admin fixes the member id before showing the invite. For a new person
  coven generates a fresh id and the admin chooses a role. For recovery the
  admin selects an existing active member; its role and circle memberships
  are preserved. The requester cannot select a different replacement target.
- E.g. Ana adds Carol, on Google Drive:
  1. Ana enters Carol's Google account email and picks her role; her phone
     shares the store's folder with that account and shows the invite.
  2. Carol's phone scans it, signs in to her own Google account, makes
     Carol's member keys, and writes a join request to storage, holding
     the invite id, its chosen member id, both new public keys and her device's name.
  3. Ana's phone shows the request, and Ana approves it.
  4. Ana's phone seals every store key it holds to Carol's new sealing key
     ([§11](#11-keys)), then writes "add member Carol" to the store log.
     - Every key, so Carol reads a late write made under an older one.
  5. Carol's phone opens the store key, adds itself to the store log, and
     Carol writes down her own restore code.
- The join request is stored at `<store>/join-requests/<invite id>`.
  - It is encrypted with a key derived from the invite secret, and signed
    with Carol's new member key.
  - Ana's device holds the secret too, so it opens and checks the request.
    Verify with the supplied new signing key, require the member id to match
    the invite, and validate both keys as fresh. Approval binds the exact
    checked pair: Add member for a fresh id, Replace member keys for an
    existing one. A changed request requires new approval.
  - The provider sees only an encrypted object under the invite's id.
- The invite only lets a device ask; Ana's approval is what lets Carol in.
- Only the device that made the invite holds its secret, so only it shows
  and approves the invite's requests.
- Carol starts at the inviting writer's key-copy number plus one, GETting
  new copies whole under the shared backoff. Their clear prefixes let her
  match both her member id and sealing key, skipping other credentials and
  audiences without opening their boxes. She
  retains her progress and stops each attempt at the next-number miss.
  The approving device publishes her initial-key copy after the invite's
  number, plus its available historical store and authorized circle keys,
  before the binding entry, even if the store has rotated. The initial key
  is required; if unavailable approval waits. Each prerequisite copy names
  the validated new sealing credential, even before its binding is current.
  No polling iteration scans every device's entry 1 while she waits.
  - Ana's invite names K1, her phone and copy 12. Copy 13 goes to Ben;
    Carol reads its prefix and keeps looking from 14. Ana approves with
    Carol's K1 copy at 14, then supplies K2 and every other historical key.
  - That starting number is only for the approval wait. Once admitted,
    Carol's ordinary bootstrap reads every writer's permanent copy log
    from 1, reusing copies already retained. Copies before the invite still
    determine exposure; the polling position cannot certify that history.
- Carol's phone learns the outcome from storage:
  - her store key sealed to the exact new credential, then a kept binding
    from the approving writer for her member id and both requested keys:
    she can register her new device;
  - her request deleted with no key sealed to her: declined or expired.
  - It reads the store log again after observing a deleted request: approval
    may have happened after its preceding observation. First refresh the
    inviting writer's copy log through a miss after the deletion; the
    approval copy may also have raced the earlier probe. Sealed keys without an
    effective matching binding keep it waiting; a dropped addition or
    replacement reaches the app with its replay reason. A losing replacement
    is a rejected join even when copies exist and its target remains active.
    Neither existing membership nor another writer's equal-payload winner
    changes the fate of that approval. The invite already names its writer;
    no requester-chosen author or target is accepted.
- Before sending, Carol's phone keeps its new member keys and exact request
  bytes in its unpublished local directory, encrypted with the invite secret
  and bound to the store, invite and chosen member id. Restarting with the same invite and
  device name reuses them and the device id. These are not final key custody.
  - It records the publication attempt before issuing it. If the attempt's
    reply is lost and neither a request nor sealed keys exist on retry, it
    treats the invite as settled. Recreating that request could revive an
    invite the admin already declined. An admin can issue another invite.
  - Provider permission failures reach the app even when revocation followed
    decline or expiry; an unreadable request is not an absent request.
- The entry adding a new Carol records how she reaches storage, her provider
  account or the id of the S3 key made for her and who holds it, so any admin
  can take it back later ([§13](#13-removing-members-and-devices)).
- Rebinding an existing id changes no access record or device id. The new
  installation registers its own fresh device after the binding is kept.
  Any changed provider access is separate work under §13, with its own
  recorded access entry; a key replacement never implies provider cut-off.
- Once the request is approved or declined, or the invite expires, Ana's
  device deletes the request object.
- Declining the request, or letting the invite expire after a day, takes
  back the storage access it granted; on S3, the key becomes pending deletion
  under §13, through coven with admin access or through the provider's console.
  Replacement-invite cleanup revokes only that invite's new grants; it
  cannot revoke the existing active member's retained access. Provider
  shared-account and retained-grant checks still apply (§13).
  - A day is by the inviting device's clock, and the invite expires when
    that device is next online after it.

### 12.3 Losing everything

- An admin re-invites a person who lost every device and their restore
  code.
  - The invite selects their existing active member id and binds their new
    keys to it after approval. Their role, circles and existing device ids
    stay attached to that id. Old credentials are exposed (§11), and old-key
    writes and entries obey their own landing windows without rescue (§10).
  - A removed member cannot be restored by replacing keys. Adding a new
    person instead uses a fresh id and the ordinary admission rules.
- Losing every member's devices and restore codes loses the store for good,
  since everything is encrypted.

## 13. Removing members and devices

- Once a kept removal has been final for more than one day and five minutes,
  drain each retired device's three logs once and stop polling them (§6).
  Until then, poll them like active devices.
  This log drain does not wait for provider cut-off; the one-time file-folder
  scan still requires confirmed cut-off (§16.5). If replay restores the
  device after removal drops, poll it again; clear any drain record based
  on the dropped entry.
- A removed device's writes count only before it read the kept retirement
  and if they landed within 30 storage days of it (§10). This applies to a
  member removal's devices too, independently of provider cut-off. Replaced
  ids have the same write deadline. Dropping the retirement recomputes the
  write verdict; entries keep §9's permanent landing rule instead.
- Removing a device, such as Ana's lost phone, is an entry in the
  store log ([§9](#9-members-and-roles)), and cuts the device off from storage.
- Providers can't cut off one device alone, so Ana cuts off all of hers,
  and signs in again on the ones she keeps:
  - Google Drive, Dropbox and OneDrive: she removes the app's access from
    her provider account;
  - iCloud: she removes the phone from her Apple account;
  - S3: an admin desktop with the storage account's admin access, usually
    the owner's, creates a new key for Ana and delivers it to her remaining
    devices. That desktop records the replacement for Ana in the store log,
    then deletes the old key. The admin-access and guided console paths are
    in §4. Ana keeps the new member key separately; her restore code does
    not change. Her own scoped member key need not be able to create keys.
  - The S3 replacement is one operation (§18). With admin access it makes the
    key and checks it from the member's side as an invite's key is checked
    (§4), seals it to Ana's sealing key and signs it as a delivered key at
    `<store>/access-keys/<Ana>/<attempt>` (D9, D10), and only then records
    the key for Ana in a set-access entry. Once that entry is kept, it deletes
    the old key from the entry's recorded view and reads that it is gone. A
    console-made old key, or a deletion that fails, stays pending
    `DeleteAccessKey`; the call reports which. If replay drops the entry, the
    operation finishes with that refusal, and the key it made stays pending
    deletion with the old one (above).
  - Each of Ana's devices, on its next sync, opens a delivered key whose
    author also authored a kept set-access entry recording that key as Ana's
    current access, and installs it as its storage key. A device that holds
    the recorded key already reads nothing. A device that did not sync
    between delivery and the old key's deletion can no longer reach storage
    to fetch it; Ana enters the key she kept. Deleting promptly is the point:
    the lost phone holds the same old key.
  - After a restart, a replacement that has not yet made its key or deleted
    the old one waits for admin access again (§4); none is stored.
  - Discarding a replacement leaves no key unrecorded. Once its entry is
    reserved, the discard publishes it and the store log covers the key. Before
    that, a made key becomes pending `DeleteAccessKey` in the transaction that
    removes the operation; with no key yet, what the attempt may have made is
    rolled back with admin access, and without it the operation waits for
    admin access with only that rollback left.
  - Recording an S3 replacement makes the old key from its recorded view
    pending `DeleteAccessKey` work in the same transaction as the entry and
    replay, even if the replacement entry drops. Recording the same key id
    again is not a replacement of that credential. Any recorded key displaced
    by replay, including a losing concurrent replacement, becomes pending too.
    The winning key stays in use unless it was already pending deletion;
    replay cannot cancel that deletion, so that case needs a fresh key.
- Device removal alone changes no member keys: the phone still holds Ana's
  private pair, but can't reach storage
  to read or write anything new.
- Replace member keys is a separate store-log entry. It preserves the
  member's access record and all device ids; it neither drains their logs nor
  certifies their file folders closed. Its old-key write deadline and send
  bound are §10's credential rules. Stored entries keep §9's permanent rule.
  Rotate exposed audience keys under §11 and handle provider credentials
  separately. Recovery with changed S3 credentials records a separate
  set-access entry signed by the rebound member or an admin under §9's
  recorded-view authority; until it is kept, the invite retains the new grant
  for failure/decline revocation.
- Removing a member is a store-log entry naming that member. It removes
  them and their devices; it contains no store key or circle-key list.
  Replay still derives deletion of circles whose sole member the author
  removed (§9). Key selection and rotation then follow §11.
  - Ana removes Carol, who shares Gifts with Ben. Ana is outside Gifts:
    she makes no Gifts key. Ben's device sees Carol's old copy and rotates
    Gifts before a first send. If Ben is offline, no remaining device sends
    new Gifts data until a member can supply a usable key.
- Applying a removal or access entry records the resulting provider work in
  the same transaction as replay. This is the only path that schedules a
  member's grant or revocation; the removal call does not do it again.
  The owner's device performs it in the pass's second phase. Elsewhere the
  pending list names the owner wait; on S3 it names the keys to delete.
- `remove_member` returns `()` once its entry is kept. Provider state is read
  only from the pending list, including retained grants and shared-account
  waits. Absence of an access record means no unresolved access work.
  - Ben removes Dan while Ana, the folder owner, is offline. Ben's call
    returns after replay keeps the entry. His list shows the owner wait;
    Ana's next pass records and performs the revocation.
- Taking back a removed member's access covers every access recorded for
  them in any entry the store log holds, kept or dropped: create-store and
  add-member entries naming them, and set-access entries authored by them or
  by an admin recording their replacement key.
  Dropping an entry does not undo the provider access it records.
  - Every distinct S3 key id gets a `DeleteAccessKey` pending record. It clears
    only after coven deletes that exact key with admin access and a subsequent
    provider read confirms that key id is absent, or after an explicit
    `confirm_access_key_deleted` call. A successful delete reply alone, a
    failed or missing read, a read of another key, elapsed time and replay
    reversal cannot clear it. Retain the record and report failures.
  - A replacement concurrent with removal therefore lists both the old and
    the new key, even though removal defeats the set-access entry in replay.
    Two admins' replacements, or an admin's and the member's, retain every
    distinct key: before removal the old and losing keys are pending, and
    removal adds the winning key. S3 access replacements use the ordinary
    timestamp conflict rule, without the priority for an admin replacing
    signing/sealing keys.
  - Every provider account is revoked using the existing owner, shared-account
    and retained-grant rules ([E9](api.md#e9-members-and-devices)).
  - Applying a later access entry for a removed member records its revocation
    work in the same transaction, even when that entry is dropped and the
    removal's original operation has finished.
  - An invite's recorded access is treated the same way when the invite is
    cancelled, declined or expires; its S3 key remains listed until confirmed.
- What the entry names is fixed when the removal starts, from the member
  list the device has then; if the replay drops the entry, the removal
  starts over against the new list ([§18](#18-operations)).
- Account sharing follows replay both ways. If a removal drops and the
  member is back, the owner's device grants the intended access again.
  - E.g. Ana owns the folder; Ben and Carol are its admins. Ana's phone
    applies Carol's removal of Ben and revokes Ben's share. Ben's earlier
    concurrent removal of Carol then wins. Ana's phone finishes its request,
    then grants Ben's share again; the app sees the pending work.
  - A grant that was already delivered cannot be undisclosed. Keys and
    storage cut-off bound what that access can reveal (§11, §13).
  - On S3, key deletion is irreversible and a pending deletion cannot be
    cancelled by replay. If a returning member's recorded key was deleted or
    is pending deletion, an admin supplies and records a fresh replacement (E9).
- The member whose provider account holds the store can't be removed:
  the store would go with their account. Removing them fails with
  `SyncError::StoreOwner`.
- On S3, `pending()` retains each `DeleteAccessKey` record until one of the
  two clearing events above ([E9](api.md#e9-members-and-devices)), whether it
  arose from replacement, removal or an invite ending. Each record names the
  key's principal: the non-secret handles its deletion needs, such as an IAM
  user or a GCS service account and managed folder (D6). A key made by hand in
  the console names none, so only the console deletes it. Retain both pending
  work and confirmed key ids through restart. Another entry, invite or retry
  cannot bring a confirmed deletion notice back. Recording entries, replay,
  pending work and confirmations uses atomic local commits; a failed commit
  reports failure and preserves the previous state.
- A removed member's old keys cannot read other members' writes first sent by a device
  that already knew of the removal, subject to §11's window for copies made
  after its copy observation (§3). Earlier attempted writes keep their keys on retry;
  provider revocation cuts off access to those objects.
- A circle the removed member was alone in is deleted by the same entry:
  no one is left who could read it ([§14.7](#147-deleting-a-circle)).
- Adding a different member concurrently with a removal does not conflict.
  - E.g. Ana adds Carol while Ben removes Dan. Both entries apply.
  - Devices share the historical keys with Carol. Any key that reached
    Dan is retired for first attempts, and devices rotate as needed (§11).

## 14. Audiences

- A *circle* is a group of members inside a store who share rows the other
  members can't read ([§14.3](#143-circles)).
- Every synced row has an *audience*: the store, or one circle.
  - A row in the store reaches every member's devices.
  - A row in a circle reaches only that circle's members' devices.
  - E.g. a household's notes are the store's, and each person pins notes
    in a circle of their own.
- The app declares, for each synced table, how its rows get their audience:
  - a *root* table names a column holding each row's audience: `store` or
    a circle's id, and never NULL;
  - a *descendant* table names one foreign key, and each row takes the
    audience of the row it points at;
  - a table that declares neither is in the store, every row of it.
- Audience is defined once, from these declarations, and everything that
  decides where a row goes uses that one definition: writes, snapshots and
  files.
- Downloaded writes and snapshots use the same schema checks for row keys,
  columns and references. An independent key must hold a canonical UUID
  (§8.5), a root's audience cell must match its row, and a descendant's
  audience reference must name a parent in that same audience. Inserts and
  present snapshot rows supply their key, reference and audience cells;
  updates may omit unchanged cells.

### 14.1 Roots and descendants

- A descendant's declared foreign key is one column, into a synced table.
  - Its action can't be set null, since the row would lose its audience;
    coven refuses it, checked when the database opens and after migrating.
  - Following declared foreign keys from any descendant reaches a root,
    or a table in the store; a loop is refused, checked when the database
    opens and after migrating.
- E.g. todos are descendants of lists, labels are in the store, and the
  join table `todo_labels` declares `todo_id`:

  ```
  lists         root         audience column: store or a circle
  todos         descendant   through list_id
  todo_labels   descendant   through todo_id
  labels        store
  ```

- A unique constraint or shared key ([§8.5](#85-keys-and-uniqueness)) must not span audiences, since a
  device outside a circle can't see the circle's claim to a value.
  - On a root table, it must include the audience column, e.g.
    `UNIQUE(audience, title)`, so the same title in the store and in a
    circle are two separate claims.
  - On a descendant table, it must include the foreign key the table takes
    its audience from, since that parent fixes the audience.
  - E.g. `note_tags(note_id, tag_id)` is allowed: only someone who can see
    note 42 can tag it, so two rows `(42, "urgent")` are always in the same
    audience.
  - E.g. `UNIQUE(file_name)` on attachments is refused: Ana's `plan.pdf` on
    a circle note and Ben's on a store note would clash on Ana's device
    but not on Dan's, outside the circle; `UNIQUE(note_id, file_name)` is
    allowed.
  - Coven checks this when the database opens and after migrating,
    and refuses to open with an error naming the table and the constraint.

### 14.2 Moving rows

- A row moves when its root's audience column changes, or a descendant is
  pointed at a parent with another audience.
- A move is a delete of the root and its descendants in the old audience,
  and a full insert of them in the new one.
  - So devices in the new audience receive the whole subtree, not changes
    to rows they never had.
  - Devices that read only the old audience see a delete.
  - Which rows move is worked out against the database as it was before
    the write.
  - Moving them back later is a re-add ([§8.3](#83-deletes)).
- A move is an ordinary write, such as `UPDATE notes SET audience = …`.
  - It commits at once on the moving device.
  - Its moved rows' file references stay fixed: each file's key
    travels in its row ([§16.1](#161-kinds-and-where-files-are)).
- E.g. Ana moves note 42 and its attachments from the store into her
  circle: Ben's devices delete them, and Ana's insert them in the circle.
- A row's generations ([§8.3](#83-deletes)) are counted per audience, so
  `_coven_rows` has one row per table, key, audience and generation.
  - A move ends the row in one audience and starts it in the other.
  - E.g. Ana moves note 1 into her circle; Ben, outside it, sees it deleted
    and re-adds note 1 in the store.
  - The store's note 1 and the circle's note 1 are two rows, each with its
    own generations, and Carol's edit in the circle changes only the
    circle's.
- When one key is present in two audiences on a device, the app's table
  shows one of them, and the other is removed like a unique value's loser
  ([§8](#8-merge)):
  - the store's row wins over a circle's;
  - of two circles' rows, the one whose current generation started with
    the smaller timestamp wins, the write `_coven_rows` records for it.
  - A device in both circles can show a different row for that key than a
    device in one, since each reads different rows.
  - E.g. Ana moves note 1 into her circle; Ben, outside it, sees it deleted
    and re-adds note 1 in the store: Ana's devices show the store's note 1
    and take out the circle's.

### 14.3 Circles

- Circles are made, and members added to and removed from them, by entries
  in the store log, under its rules ([§9](#9-members-and-roles)).
- Any member of the store can make a circle, and is its first member.
- A circle's own members rename it, add and remove its members, and delete
  it ([§14.7](#147-deleting-a-circle)); an admin outside the circle can't.
- Concurrent renames don't conflict: both entries are kept, and the name is
  the latest kept rename's in timestamp order, as for edits to one cell
  ([§8.2](#82-concurrent-writes-to-one-row)). A rename that has read another
  has a later timestamp. Renames still conflict with deleting the circle
  ([§9](#9-members-and-roles)).
- Each circle has its own key, sealed to each member's current non-retired
  sealing key, with both member id and sealing key authenticated in each copy,
  like the store key ([§11](#11-keys)).
  - Its sealed copies live at `<store>/keys/<writer>/<n>`, with the circle,
    key id and recipient in the clear prefix ([§11](#11-keys)).
  - It is replaced whenever someone leaves the circle.
  - Someone joining a circle gets its earlier keys too, so they can read its
    history.

### 14.4 Writes

- A circle's row changes go in the same device logs, encrypted with the
  circle's key.
  - A write that changes rows in the store and in a circle keeps each part
    encrypted with its own key, in one object.
  - A device applies the parts it can open, and skips the rest.
  - E.g. Ana adds a note and pins it, in one transaction:

    ```
    <store>/devices/ana-phone/12
      header, sealed with the store key:             ana-phone, write 12, timestamp, had read …
      part 1, chunk 0, sealed with the store key:    notes  row 47  insert "Paint colors"
      part 2, chunk 0, sealed with Ana's circle key: pins   row 4   insert note → 47
      signed with Ana's key, over all of the above
    ```

  - Ana's other devices apply both parts in one transaction; Ben's apply
    part 1, and can still check the signature, which covers part 2's
    encrypted bytes.
- A skipped part counts as applied, so a device never waits on a write it
  can't read ([§7.1](#71-causality)).
  - A device has applied every entry the write had read, so it knows each
    part's key; it skips a part only when it isn't in that key's audience.
- When replay makes a circle readable again, reload everything skipped
  while it was unreadable. A deleted circle returning follows the same rule,
  as does one whose creation dropped and returns. A circle whose creation
  arrives with the entries that make it readable has nothing skipped: every
  write with a part in it read that creation. Nor does a circle whose only
  earlier creation landed too late, since no view keeps that entry.
  The [returning-circle proof](proofs/storelog-data.md#returning-circles)
  derives both facts from replay and the write's recorded-past checks.
  - Do not use the device's overall passed positions as proof that it read
    those parts. Load the circle's usable snapshot and remaining history.
  - Align it with the other audiences at the common point in §15. Commit
    the reload, merge records and positions together before allowing new
    app writes into the circle; until then they return
    `DbError::AudienceReloading`. Other audiences remain usable.
  - Missing keys, snapshots or history appear in the pending list. The
    circle remains unavailable until the reload can commit.
  - A circle that replay leaves unreadable before its reload commits has
    nothing to reload: that replay removes the requirement and its pending
    record (§19.1), and the circle's next return requires the reload again.
  - E.g. Ben's laptop skips Ana's Gifts writes 9 and 10 after his removal.
    When Ben rejoins, it loads both even though its log position is 10.
    The same happens if Gifts was deleted and that deletion later drops.
- A part sealed with an authorized historical key counts like any other
  part. Current audience members wait for their copy before applying it.
  - Ana removes Ben, then Carol rotates to K2 and writes with it. Ben's
    concurrent removal of Ana can return Ben to the audience. Carol shares
    K2 with Ben; he applies those same parts. No applied value is taken back
    merely because membership changed around the key's introduction.
- All members can see that a circle exists, who writes to it, when, and how
  much.

### 14.5 References

- A row may only point at rows that everyone who can read it can also
  read.
  - A circle's row may point at rows in the same circle, or the store's.
  - A store row may point only at store rows.
- Coven refuses a write that breaks this, on the device making it.
  - That includes a move: moving note 42 into Gifts while store link 5
    still points at it is refused with `DbError::ReferenceAudience`, since
    link 5 would point at a row Ben can't read.
- Coven also refuses, with `DbError::NotInCircle`, a write that puts a row
  in a circle this member isn't in, or in no circle the store log has.

  ```
  store          notes  row 42   "Groceries"
  Ana's circle   pins   row 3    note → 42        allowed
  store          links  row 10   pin → 3          refused: Ben can't read pin 3
  Ben's circle   pins   row 2    pin → 3          refused: Ben can't read pin 3
  ```

- So every device that reads a row can check its foreign keys, and
  [§8.4](#84-foreign-keys) applies unchanged.
- E.g. when note 42 is deleted, the devices in Ana's circle apply its
  foreign key's action to pin 3.
- Which circle a row is in never depends on who is in the circle, so a
  change of members never breaks a reference.

### 14.6 Leaving a circle

- E.g. Ana and Ben share a circle, and Ana removes Ben from it.
  - The removal entry names Ben and the circle. Ana's device then rotates
    the exposed circle key, sealing its replacement to Ana alone.
  - Ben keeps the rows he already had. Remaining members' devices use a
    fresh key for first attempts once they know he has left (§11;
    [checked history: `remaining_member_rotates`](proofs/queue.md#checked-histories-and-limits)).
    Earlier attempted writes keep their original keys
    ([queue proof: `Run.retries_identical`](proofs/queue.md#what-is-proved)).
- Removing a circle's last member in the author's view deletes the circle
  ([§14.7](#147-deleting-a-circle)).
- Concurrent removals can also leave no members after the full replay,
  even though each author saw someone remaining.
  - Treat that circle as deleted while its resulting member list is empty.
    This is computed after replay, not an extra conflicting entry.
  - Its deleted-circle cause names the latest kept removal affecting that
    circle, by timestamp. Keep every contributing entry's inputs until final.
  - E.g. Ana's phone removes Ben from Gifts while her tablet removes Ana
    from the store; both had seen Ana and Ben in Gifts. Both removals apply
    if another store admin remains, and Gifts is empty and hidden.
  - Carol's concurrent addition to Gifts, if it arrives, can leave it with
    Carol instead. The rows return and Carol reloads their history (§14.4).
- A write Ben made to the circle before he had read his removal passes
  this circle-membership check, as with a concurrent entry (§9). It still
  follows §10's device-retirement checks, §17.1's schema rules and §8's
  per-change data rule.
  - Leaving a circle does not retire a device or start a device-retirement
    window. A separate removal of Ben or his device, or replacement of its
    id, does (§10).
  - Ben is still in the store, so storage access doesn't stop him sending
    his own pre-removal edits, including their first attempts, with the old
    circle key he already received (§6). This exposes no new circle data
    to him; he cannot make new writes into it
    ([checked history: `departed_member_counterexample`](proofs/queue.md#checked-histories-and-limits)).
  - Only trust keeps him from claiming he hadn't read his removal, and
    members are trusted not to be hostile ([§2](#2-threat-model)).
- Once Ben's device reads his removal, it keeps the circle's rows it has,
  refuses new writes into the circle with `DbError::NotInCircle`, and no
  longer snapshots or fingerprints it.

### 14.7 Deleting a circle

- Any member of a circle can delete it.
- Deleting a circle publishes one store-log entry (§9). It makes no row
  write and advances no row generation.
- The deleted-circle rule hides every row in that circle while the replay
  keeps the deletion, including rows arriving later.
  - The loss record names `DeletedCircle` and the entry responsible.
    It describes a deleted circle, not a conflicting edit that lost.
  - Values and merge records remain available if the entry is dropped.
    Then the rule disappears and the rows return, unless another rule
    still hides them.
- A device that has applied the deletion refuses new writes into the circle.
- E.g. Ana and Ben share Gifts, holding notes 7 and 8. Ben deletes it while
  Ana's phone, which has not received the deletion, adds note 9.

  ```
  <store>/store-log/ben-phone/4   delete circle Gifts
  ```

  - All three notes are hidden under the same deleted-circle rule.
  - Ana's app can offer to put note 9 elsewhere by inserting it with the
    same key (§8), so a returning circle cannot create a second copy.
  - If Ana concurrently removed Ben from the store, her higher-tier entry
    wins. Ben's deletion drops, and all three notes return on replay.
- Devices outside the circle read only the entry, never its row values.
- A row hidden by circle deletion keeps its local file sources and its
  queued files keep uploading under the ordinary file rules. Hiding a row
  neither pauses its queue nor changes its fixed file reference.
  - Ana attaches a photo before Ben's Gifts deletion arrives. The photo
    still uploads while its row is hidden. If the deletion drops after Ana's
    phone goes offline, the returning row's photo is already in storage.
  - A real row delete releases sources only under the ordinary reference
    and finality checks; a circle deletion is not such a row delete.
- Physical cleanup waits for the deletion's finality (§9) and the snapshot
  and file-reference checks (§15, §16.5). Loss records that still name a
  file retain it.

## 15. Snapshots

- A snapshot is the synced tables and coven's merge tables
  ([§8](#8-merge)) as one device has them, encrypted, with how far into
  every log they reach.
  - A device's own `_coven_uploads`, `_coven_operations` and `_coven_pending`
    aren't in it. Loading keeps that local state, except that committing a
    changed reset suppresses pending records (§19.1). Their inputs
    remain until that reset is final (§9).
  - Snapshots live at `<store>/snapshots/<audience>/<device>/<n>`, where the
    audience is `store` or a circle's id.
  - Its prefix, outside its encryption, names its audience, its key,
    its catch-up storage time `T_c` (D9), and its positions, so any device can
    choose one and decide what it covers by verifying the prefix's own member signature, without opening
    its encrypted data. Unverified positions never establish coverage.
  - A snapshot is one object, encrypted in chunks like a write
    ([§6](#6-syncing-writes)), and written and loaded a chunk at a time.
    Its author's member key signs both its prefix and its complete sealed
    bytes. Every read resolves the device's owner and current signing key
    in the snapshot's signed `store_log` view for both signatures. Until
    verified that view is only a lookup hint. Loading also verifies the
    complete object's signature before applying anything from it (D9).
    Key replacement alone does not invalidate a historical signature or
    impose a 30-day snapshot deadline. The signed catch-up-time deadline and
    every coverage/retention check still apply. This snapshot policy is
    separate from the proved entry/write window rule.
  - A snapshot *covers* a write when the write is within its positions:
    `<store>/snapshots/store/ana-phone/3` covers ana-phone's writes 1 to 40.
- Snapshot loading preserves each change's actual generation/setter sources,
  own admission evidence and original-value losses (§8, D7). Consumed
  coverage is not evidence that a covered change survived. Dismissal cannot
  remove source evidence. A changed replacement decision rebuilds admission,
  rows and losses in the loading transaction; retain original inputs until
  the deciding replacement prefix is final and independent retention checks
  pass. A snapshot cannot rescue an old-key write by covering it.
- Snapshots also retain the original creator identities and verified direct
  and foreign-key provenance required by §8.8. A snapshot author's signature
  cannot substitute for another member's original authorship. Recheck exact
  derived effects from retained original schema and execution evidence before
  accepting foreign-owned row changes; losing or dismissing a value does not
  erase the evidence needed for that decision.
- Snapshots are discovered by listing `<store>/snapshots/<audience>/`,
  never by GETting the next snapshot number. Retention can delete snapshot
  2 while leaving 3. List the audience's retained snapshots and check their
  signed prefixes by range to choose the newest under the ordering below;
  cached checked prefixes need no second read. Fetch a body only to load it.
- A new device, or one that fails the recent-return check below, performs
  this discovery for each readable audience before treating write misses as
  no new work. Apply the newest usable snapshot only if its signed write
  positions go past where that audience left off on some writer. Otherwise
  keep the local state and continue from its next write numbers. The atomic
  common-point loading and schema/reset rules below still apply.
  - Ben left at Ana's write 8. On returning after 40 storage days, he lists
    the store's snapshots and checks the newest prefix: it covers Ana 20.
    He loads it, then asks for 21. If it covered no writer past his local
    positions, he would keep his state and resume normal log reads.
- Keep the storage time `S` certified by the last completed catch-up pass
  with its applied positions. For this purpose, completion means discovery
  reached each undrained log's terminal miss, with completed drains supplying
  the others (§6), required reloads and received writes committed, and the
  corresponding positions were confirmed posted. A
  finished pass with unresolved history does not move this checkpoint.
  A pending file or retention operation alone does not prevent it.
  - On return, replace this device's clock object and read its storage time
    `T`. Pair it with a monotonic reading taken before starting the clock
    replacement, so elapsed time includes the replacement, status request
    and sleep. Reusing this sample keeps that same monotonic starting point.
    No saved checkpoint means new. A failed time observation or missing
    monotonic evidence cannot justify the recent-return rule.
  - The identity check comes before that replacement (§10). On reopening,
    use the read-only snapshot/positions evidence first; when away is not
    yet known, conservatively inspect the readable snapshot folders too.
    Reuse those observations after the clock result, without relisting.
  - During a running session, a monotonic timer schedules the next clock
    check before the recent-return deadline below. A suspended session
    refreshes storage time on return; reopening cannot reuse a preceding
    session's monotonic reading. Each qualifying pass can reuse a storage
    sample known before its discovery as `S`; keeping an older sample is
    conservative. An idle pass neither rewrites its clock object nor commits
    an unchanged checkpoint.
- At each use of an unrequired write's next-number miss, check
  `T + monotonic elapsed < S + 29 days`, with elapsed measured from the
  reading paired with `T`, including sleep. Only while it holds does the
  recent-return rule let the miss mean nothing new. Check when using the
  result, including a cached or parallel read's miss, not just at pass start.
  Under [§4](#4-storage-providers-and-access)'s storage-clock rate assumption,
  this one-day margin keeps current storage time below `S + 30 days`, the
  storage-age deletion threshold.

  At equality or later, use the away-device path: discover snapshots and
  complete any required reload before using further write misses. A snapshot
  listing from before expiry cannot certify misses after expiry. A failed
  discovery or reload leaves history pending and keeps the old checkpoint.

  Earlier work was already consumed at the saved checkpoint, and a write
  published since that pass is protected while the check holds. Reader
  positions cannot release an unconsumed write either.
  Required history not supplied by the reload rules below remains pending;
  an unresolved gap never counts as a successful catch-up
  ([`scan_spec`](proofs/io/CovenIO/FixedReload.lean)).
  - **Ben's pass crosses the deadline.** His checkpoint is day 0. A pass
    starts on day 29, but its read of Ana's day-1 write returns a miss on
    day 31, after retention deleted it. The start cannot certify that miss:
    the recent-return check already fails at day 29, and Ben must discover
    snapshots before using it. A pass starting before day 29 must take the
    same path if it crosses the deadline before using a miss.
- A device writes one for an audience once that audience's parts after
  its latest snapshot add up to more bytes than that snapshot, or than
  1 MiB while the audience has none.
  - Both sizes count encoded plaintext, before sealing adds chunk overhead.
    The latest snapshot's size follows from its listed object size and D9's
    fixed chunk layout. Choosing it uses its cached verified prefix, reading
    that prefix only when its checked local record is missing. Growth sums
    the part lengths recorded when writes were authored or applied; neither
    decision rereads unchanged headers or decrypts snapshot rows
    ([One sync pass](sync-pass.md#what-survives-a-pass)).
- Retention runs every pass: for each final write that passes the reader
  positions or 30-day age gate below, request a fresh snapshot in this pass
  or the next for every readable part audience whose current newest
  counting snapshot does not cover it, including an audience with no
  snapshot at all
  ([`RetentionDeadline.refreshDue`](proofs/io/CovenIO/RetentionDeadline.lean),
  [`AudienceRetention.first_witness`](proofs/io/CovenIO/AudienceRetention.lean)).
  - E.g. Ana's only write is final and 30 storage days old, but below the
    1 MiB growth threshold and never snapshotted: retention requests its
    first snapshot, then waits for that snapshot's one-day age
    ([`first_snapshot_deleted`](proofs/io/CovenIO/CommonReloadExamples.lean)).
- Snapshot growth uses the latest checked local snapshot facts. Only when
  growth would request a new snapshot, a reload needs one, or a retention
  candidate becomes due, refresh the relevant audience's snapshot listing
  and read previously unchecked signed prefixes. Re-evaluate growth or
  deletion against that coverage; an idle pass does none of this discovery.
  A wait with unchanged prerequisites uses the shared backoff rather than
  listing again on every pass. Retention reads an uncatalogued candidate's
  header at its exact write path; it never lists historical write objects.
- Prepare a snapshot only immediately after a catch-up has read every
  writer's log through its next-number miss, with all required history
  applied and nothing pending in that catch-up; a reload invalidates the
  earlier terminal misses
  ([`completed_catchup_covers_old`](proofs/io/CovenIO/Reload.lean)).
  - Sample storage time `T_c` before those reads, and keep that sample in
    the signed prefix throughout preparation and upload, including retries.
    [`SnapshotPublication.begin_samples_time`, `read_uses_prior_sample`,
    `prepare_preserves_sample`, `land_preserves_sample`](proofs/io/CovenIO/SnapshotTiming.lean)
    prove sampling and preservation in the publication model. Signed-prefix
    encoding and exact-byte retries are not proved by these theorems.
  - At preparation, the new snapshot must cover every position of that
    audience's current newest counting snapshot; this check does not lock
    the remote catalog or require covering a concurrent arrival at landing
    ([`SnapshotPublication.canPrepare`](proofs/io/CovenIO/Retention.lean),
    [`SnapshotRefresh.refresh_outranks`](proofs/io/CovenIO/SnapshotRefresh.lean)).
  - A snapshot *counts* only if its first complete publication time is at
    most `T_c + one day`; readers ignore later landings for loading,
    selection and coverage, including snapshots named by raises or resets
    ([`late_snapshot_ignored`](proofs/io/CovenIO/Retention.lean)).
  - Once an attempt passes that day without confirmed counting publication,
    abandon it and catch up and prepare afresh at a new snapshot path;
    stopping locally does not prevent the old request landing remotely.
    [`late_snapshot_ignored`](proofs/io/CovenIO/Retention.lean) proves that
    selection ignores a late landing. `SnapshotPublication.act` permits a
    fresh path while an old request can still land, but has no local
    confirmation or abandonment state: the abandonment rule is not proved.
  - Exactly one day counts, but nothing here guarantees an upload duration
    or eventual successful catch-up
    ([`Snapshot.counts`](proofs/io/CovenIO/Retention.lean),
    [`AudienceRetention.first_witness`](proofs/io/CovenIO/AudienceRetention.lean)).
  - E.g. Ana samples noon Monday, completes the catch-up and prepares a
    snapshot covering the current selection: a landing at noon Tuesday
    counts, while a landing one second later is ignored
    ([`late_snapshot_ignored`](proofs/io/CovenIO/Retention.lean)).
- Before sending any snapshot, including a raise snapshot, confirm every
  own write through its captured position; remote positions must come from
  checked published log prefixes or previously published snapshots subject
  to the same gate
  ([`snapshot_covers_published`, `inherited_position_published`](proofs/io/CovenIO/SnapshotWrites.lean)).
  - Local capture may precede own-upload confirmation, but neither capture
    nor coverage settles a queue slot; confirmation requires the original
    bytes at its path, and later app writes do not extend the captured cut
    ([`publication_after_confirmation`](proofs/queue/CovenQueue/RaiseUpload.lean),
    [`confirmation_requires_equal`](proofs/queue/CovenQueue/Upload.lean)).
  - E.g. Ana captures her two-part write 1 in a store snapshot while its
    log path is absent: she must upload and confirm write 1 before sending
    the snapshot, so a Gifts loader cannot mistake an unpublished part
    for deleted history
    ([`pending_run`, `two_audience_raise_keeps_circle_part`](proofs/io/CovenIO/SnapshotWriteExamples.lean)).
- Before uploading a snapshot, its writer opens the sealed temporary file
  through the snapshot reader: decrypt, verify both signatures, and parse
  and check every record.
  - Its fingerprint must equal the database state the writer captured,
    at those same positions and schema version. Writes committed since
    that capture do not change the comparison.
  - A failed check stops publication and reaches the caller.
- After upload, compare the provider's checksum of the complete stored
  bytes with the local checksum, using the provider's checksum algorithm.
  - For multipart uploads, compare the checksum of the complete object,
    not a part checksum or an identifier that is not a checksum.
  - A provider without a complete-object checksum must return the stored
    bytes for this check. A mismatch is an integrity failure, not success.
- Readers still choose by the authenticated prefix; they do not certify a
  snapshot for other devices by opening its body.
  - E.g. Ana's phone discovers its published snapshot was damaged. Ben's
    healthy laptop writes a newer snapshot; new devices load that one.
  - If Carol already loaded incorrect data, a person resets from Ben's
    trusted copy (§19.3). Keeping one older snapshot is not a recovery rule.
- The *latest* or *current newest* snapshot of an audience is its counting
  snapshot covering the most writes, counted over every log; a tie goes to
  the smaller path, comparing the writing device's id and then the snapshot
  number as numbers (device 9 before device 10), and the existing schema,
  reset and reader checks still decide usability
  ([`catalogCurrent`](proofs/io/CovenIO/Retention.lean),
  [`SnapshotHistory.deleted_stays_covered`](proofs/io/CovenIO/Reload.lean)).
- Until an audience has a snapshot, a new device reads every log from the
  start; no log object is deleted before a snapshot covers it.
- Two devices can write one at the same time, and both are correct:

  ```
  <store>/snapshots/store/ana-phone/3     ana-phone up to 40, ben-laptop up to 22
  <store>/snapshots/store/ben-laptop/1    ana-phone up to 38, ben-laptop up to 25
  ```

- The larger write count selects Ben's snapshot; fetch the needed suffix
  under the reload rules below, replacing a selection if deletion races
  with those reads
  ([`catalogCurrent`](proofs/io/CovenIO/Retention.lean),
  [`committed_prefix`](proofs/io/CovenIO/CommonReload.lean)).
- A device snapshots only what it can read.
  - Any device snapshots the store's rows.
  - A device of one of a circle's members snapshots that circle's rows,
    separately, sealed with the circle's key ([§14.3](#143-circles)).
- At reload start, fix a required position for each writer: include the
  remote past of waiting local writes, already uploaded own writes and
  their past, and the positions of audiences whose existing state is kept,
  including an audience with no rows
  ([`committed_safe`](proofs/io/CovenIO/CommonReload.lean)).
  - Choose a counting usable snapshot for each readable audience needing
    replacement; for each writer, the common target is the maximum of the
    frozen requirement and every selected snapshot's position
    ([`target_ready`](proofs/io/CovenIO/CommonReload.lean)).
  - While such an audience lists a snapshot whose prefix is in a newer
    format, that snapshot may be its newest: the reload and the
    recent-return check wait for the update rather than choose without it
    ([`reload_waits_for_newer`](proofs/storelog-data/CovenStorelogData/PendingScans.lean)).
  - Read each audience forward to that target, skipping parts already
    supplied by its selected snapshot and retaining actual checked write
    bytes for the rest; stop at the target, not a moving log tail
    ([`scan_spec`, `committed_prefix`](proofs/io/CovenIO/CommonReload.lean)).
  - After a successful missing GET inside the target, obtain a fresh,
    complete snapshot catalog: counting coverage through that number in
    any audience, including one the loader cannot decrypt, establishes
    historical publication and, with the absence, a deleted number
    ([`covered_miss_published`](proofs/io/CovenIO/SnapshotWrites.lean),
    [`SnapshotHistory.deleted_miss_detected`](proofs/io/CovenIO/Reload.lean)).
  - If the loaded audience's current newest counting snapshot reaches
    that number, select and load it; otherwise skip the number for that
    audience, since a deleted write outside its current coverage has no
    part there
    ([`scan_spec`, `skipped_has_no_part`](proofs/io/CovenIO/FixedReload.lean)).
  - A failed request, incomplete catalog, missing key or permanent refusal
    cannot supply this absence certificate; unresolved history remains
    pending (the successful-source premises of
    [`scan_spec`](proofs/io/CovenIO/FixedReload.lean)).
  - E.g. Ben loads Gifts through Ana's write 8 and misses required write 9:
    a fresh store snapshot through 9 proves publication, but Gifts' current
    snapshot still stops at 8, so the deleted write has no Gifts part and
    Ben skips it for Gifts
    ([`skipped_has_no_part`](proofs/io/CovenIO/FixedReload.lean)).
- Keep each selected snapshot fixed until a certified deleted number
  forces that audience's replacement; new writes and unrelated snapshot
  arrivals do not replace the selections
  ([`retry_stale`](proofs/io/CovenIO/CommonReload.lean)).
  - A replacement changes only that audience's selection; recompute the
    common target and read all audiences to it before committing, even
    when the replacement reaches beyond the original target
    ([`committed_safe`](proofs/io/CovenIO/CommonReload.lean)).
  - Reuse deletion evidence from a selection's catalog without another
    missing GET, and keep the loaded snapshot bodies and the committing
    round's checked write inputs throughout staging; this requires no
    remote snapshot pin or publisher supplying all audiences together
    ([`scan_spec`, `common_cut_commits`](proofs/io/CovenIO/CommonReload.lean)).
  - E.g. a deleted write 1 makes the store reload through Ana's write 2,
    while Gifts' selected snapshot stays through 1: fetch write 2's Gifts
    part and stage both audiences through 2
    ([`replacement_fills_other_audience`](proofs/io/CovenIO/CommonReloadExamples.lean)).
- Commit all readable audiences at that common point in one transaction:
  rows, merge records, losses, realized positions and eligible waiting
  local writes applied in causal order on top, with the existing schema,
  reset, key and validation checks still required
  ([`committed_number`, `common_cut_commits`](proofs/io/CovenIO/CommonReload.lean);
  row application and validation are implementation obligations).
  - During staging, app reads see the old committed view and ordinary app
    writes join the waiting queue; they read no staged rows, so their remote
    past stays within the frozen requirement, and a failed reload leaves
    the prior visible state and positions intact
    ([`staging_keeps_visible`](proofs/io/CovenIO/AtomicReload.lean),
    [`committed_safe`](proofs/io/CovenIO/CommonReload.lean)).
  - When a kept reset or breaking-schema entry is received, writes into
    its audience wait for the atomic reload with `DbError::AudienceReloading`.
    Other audiences stay writable. A write must not claim to have adopted
    the boundary while still using the state it discards. An app older than
    a kept raise's version keeps queuing its old-version writes until it
    updates (§17.1); those never claim the newer boundary.
  - E.g. while the store is staged through 2 and Gifts through 1, Ben's app
    still reads the preceding committed state; it sees both at 2 only after
    Gifts' part and the eligible waiting writes commit together
    ([`staging_keeps_visible`](proofs/io/CovenIO/AtomicReload.lean),
    [`replacement_fills_other_audience`](proofs/io/CovenIO/CommonReloadExamples.lean)).
- With a fixed writer inventory and finite successful catalog, snapshot
  and required log reads, this reload terminates once deletions stop: from
  the next round boundary, at most one replacement per readable audience
  and one more finite round than replacements are needed, even if writes
  and snapshots keep arriving
  ([`terminates`, `eventually_terminates`](proofs/io/CovenIO/CommonReload.lean)).
  - E.g. with the store and Gifts, after deletion stops there can be at
    most two further replacements and three finite rounds; continued
    deletion or unbounded read durations gives no elapsed-time bound
    ([`terminates`](proofs/io/CovenIO/CommonReload.lean)).
- These positions govern device-write logs. Before loading a snapshot, the
  device has applied at least the store-log positions it names; loading does
  not replace store-log entries or rewind their applied positions.
- A snapshot from an older schema can load across additions: SQLite supplies
  the new columns' defaults. It cannot cross a breaking change, and a newer
  snapshot waits for the app's schema to update.
- Loading an audience's snapshot replaces that audience's rows and merge
  records; other audiences keep theirs, and the removal rules run again on
  rows that point at changed ones, as after any write
  ([§8.4](#84-foreign-keys)).
- Every loss travels in the same snapshot section ([D7](format.md#d7-snapshots)),
  retaining the row, column when present, written values, setters and cause.
  Frozen losses do not require current schema columns. A migration-deleted
  row keeps its generation records; the frozen values need no live cells.
  Loading preserves them in `_coven_lost`, and every loss counts in its
  audience's fingerprint (§19.1).
- Each device advances its posted positions only over fully realized work.
  It can post new pending records without advancing them (§6).
- Deleting a log object requires all of the following:
  - for each part audience, a counting snapshot covering the write landed
    strictly more than one day ago, and that audience's current newest
    counting snapshot also covers it
    ([`Eligible`](proofs/io/CovenIO/Retention.lean),
    [`SnapshotHistory.deleted_stays_covered`](proofs/io/CovenIO/Reload.lean));
  - every store-log entry that makes this coverage usable, or makes the
    object unnecessary, is final (§9;
    [`Eligible`](proofs/io/CovenIO/Retention.lean));
  - every device's posted write position has passed it, or storage has
    held it for 30 days
    ([`recent_write_protected`](proofs/io/CovenIO/Retention.lean)).
  - Each audience can have a different historical covering snapshot, and
    its current snapshot need not itself be a day old; replacing a snapshot
    does not restart the historical covering snapshot's age
    ([`maturity_survives_new_snapshots`, `eligible_after_day`](proofs/io/CovenIO/Retention.lean)).
  - While an audience lists a snapshot whose prefix this device cannot read
    yet, in a newer format or by an author whose device registration has
    not applied, its current newest counting snapshot is unknown. Deleting
    that audience's parts waits, as the reload waits for the update; that
    snapshot's own record names the condition. A refused prefix establishes
    nothing and does not hold deletion
    ([`waiting_retention_safe`](proofs/storelog-data/CovenStorelogData/PendingScans.lean);
    the registration wait is not modeled).
  - That current newest is chosen by verified prefix, whatever this device
    found in a snapshot's body and whether it accepts its key: a version
    that loads the body selects that snapshot (§19.1). E.g. Ana's version
    refuses the body of Ben's newest snapshot, which covers her writes 1 to
    3, so she keeps her write 4
    ([`refused_body_retention_counterexample`, `retention_ignores_bodies`](proofs/storelog-data/CovenStorelogData/RefusedRetention.lean)).
  - E.g. Ana's snapshot covers her writes 1 to 4. Ben, on a newer version,
    snapshots her writes 1 to 3 and more of his own, so his is the newest.
    Ana cannot read its prefix, so she keeps all four: after her update the
    reload selects Ben's snapshot and needs write 4
    ([`newer_snapshot_retention_counterexample`](proofs/storelog-data/CovenStorelogData/PendingScans.lean)).
  - Retain the checked prefix and landing time that establish historical
    coverage while a retained write relies on them, even after the snapshot
    body is removed; without that evidence, wait for a known covering
    snapshot to mature (the historical-evidence premise of
    [`bounded_witnessed_storage`](proofs/io/CovenIO/RetentionDeadline.lean);
    durable preservation is an implementation obligation).
  - E.g. Ana's store snapshot covering write 9 landed at noon Monday and
    its replacement landed Tuesday morning: with current coverage of every
    part and the other gates satisfied, deletion may run after noon Tuesday,
    but not at noon itself
    ([`eligible_after_day`](proofs/io/CovenIO/Retention.lean)).
  - Age compares storage times only: use the observed storage time T from
    §9 and the object's first stored time. A quiet store's due observation
    uses the per-device clock object in §9; unchanged positions are
    not refreshed. Device-made write and entry timestamps never establish
    storage age.
  - E.g. Ana's phone clock jumps ahead a year. A log stored yesterday is
    still only one storage day old, so that jump cannot release it.
  - Every device means every active device in the store log; removed and
    replaced devices are not waiting readers here. Finality still follows §9;
    one that has never posted counts as having read nothing.
  - When coverage and finality pass but an active reader has not posted past
    the object, its retention reason is `Waits(Positions(device))`.
    A missing or invalid post counts as no position. If several readers lag,
    name the first by device id. The wait ends when that reader posts past it
    or the object's storage age reaches 30 days; then evaluate any remaining
    reader and retention conditions.
  - Ana's snapshot covers write 9, but Ben last posted 8. Its retention record
    names Ben. Ben posting 9 satisfies his wait, as does the object reaching
    30 storage days; neither alone bypasses the snapshot or finality checks.
  - Coverage needs only the write header's part audiences, authenticated by
    its store-key encryption and bound to its path and prefix. Retention
    uses the facts retained when that device authored, applied or checked
    the write. Only missing header facts require a read; keep its result
    across passes. A header-only read needs no parts or whole-object author
    signature. These fields describe
    the object being deleted; they grant no author authority and apply no
    rows. Loading a write still checks its complete signature (§6).
- A write waiting for entries or a key copy holds retention back, with a
  `Retention` pending record naming that first prerequisite. It does not
  fail the pass. Arrival of the prerequisite permits another attempt.
- A log with a permanent refusal is not read at or past that object,
  including for retention and reload. Keep those objects and any files
  whose absence cannot be proved without them.
  - A reload requiring that gap fails atomically with `SyncError::Pending`.
  - A snapshot covering it can load without reading it. The refusal remains
    recorded until the reset or update rules of §19.1 permit a new attempt.
- A device deletes a snapshot of its own once a newer one of the same
  audience covers everything it covers, and the entries that make that
  replacement usable are final (§9).
  - A snapshot named by a kept reset or version-raise entry that changes
    the replayed state stays: new devices need its authenticated coverage
    to judge late writes and select a snapshot following that boundary.
  - Each device deletes its own log objects.
  - A removed device's are deleted by another device of the same member,
    since they were uploaded with that member's account.
  - A removed member's are deleted by a device of the member whose provider
    account holds the store.
    On S3, where access-key ids do not identify their account, the member
    recorded by the kept creation entry performs this deletion.
  - On Google Drive only an uploader can delete their files, so a removed
    member's just leave the store's folder, and stay in their own account.
  - A device that never comes back keeps its covered log objects until it
    is removed.
- A device that needs writes already deleted loads the latest snapshot
  instead, like a new device.
  - Use the fresh catalog and per-audience reload-or-skip rule above for
    a missing required write; an older cached catalog alone cannot certify
    its deletion ([`scan_retry_time`](proofs/io/CovenIO/FixedReload.lean)).
  - Its own writes still waiting in `_coven_uploads` keep their numbers, and
    it uploads them after.
  - Every device judges them under the schema and reset rules (§17.1,
    §19.3), with §10's landing deadline for retired devices. Ordinary late
    writes from active devices apply once their prior reads are covered
    or applied; pre-reset writes outside the chosen snapshot are ignored.
  - E.g. Ana's old phone made writes 31 to 33 offline, then stayed offline
    for a year:

    ```
    1. it loads the latest snapshot, which reaches ana-old-phone up to 30
    2. it downloads the writes after the snapshot
    3. it uploads 31 to 33 from _coven_uploads
    4. every device judges 31 to 33 under §8, §17.1 and §19.3
    ```

  - Without an intervening breaking change or reset, an edit to a cell
    changed since loses on its stamp, and an edit to a deleted row loses
    on its generation.
- A deleted row's `_coven_rows` row stays for good, at one small row each,
  so a write made at its old generation loses however late it arrives.

## 16. Files

- Files are what the app attaches to rows: audio, images, documents.
- The app declares, for each synced table that carries files:
  - which column refers to the file;
  - which column holds its size in bytes, which a user-provided file is
    checked against;
  - whether a row, once it has a file, may be pointed at a different one;
  - which column holds the file's *content hash*, the SHA-256 of its
    bytes, which coven fills in when a write attaches the file;
  - which column holds the file's fixed storage reference, filled by coven
    ([§16.1](#161-kinds-and-where-files-are));
  - the file's kind, user-provided or app-provided;
  - whether devices download an uploaded file as soon as its row arrives,
    or on first read.

### 16.1 Kinds and where files are

- A *user-provided* file is the user's own file at a path on their device,
  such as a song in their music folder.
  - Coven records its path, size and modification time, and doesn't copy
    it.
  - Coven never changes or deletes it.
  - If it moves or changes, coven reports it as missing or changed, before
    reading any of it.
- An *app-provided* file is bytes the app hands to coven, which keeps and
  owns them.
- The first read records both the whole-file content hash and a SHA-256
  hash of each 64-KiB plaintext chunk (the last may be shorter), when
  preparing a user-provided original or staging an app-provided file.
  These hashes stay local; they are not part of the storage format.
- The app can hand them over as a stream, so a large file never has to fit
  in memory.
- The attaching write picks the file's random id and key, records its
  uploader, and fixes its path as `<store>/files/<device>/<file>`.
  - Its where-column carries that reference from the start (D12).
    It does not say whether an upload has finished.
  - The same transaction queues the file, even with no storage connected.
    No later write marks it uploaded or changes the row when transfer ends.
  - Pinning and caching change no file reference. A `FileRef` stays valid
    across upload completion.
- File status makes a fresh single-object status call (§4), then combines
  that observation with the current uploader and reports (E8), in this order:
  - **Available:** storage contains the complete file at its fixed path.
  - **Missing:** storage confirms absence, and its uploader was removed
    or replaced, or reports that it cannot upload this file (§19.1, D8).
  - **Uploading on D:** storage confirms absence and D is still active,
    with no report that it cannot supply the file. This includes a paused
    queue or an offline uploader; it does not promise current byte transfer.
  - A network or permission failure is an error, never proof of absence.
    With no storage configured, the call returns `NoStorage`.
  - Storage presence wins over an old missing-source report. A report's
    absence does not mean the object exists.
- A read can still use a checked local source or cached bytes. If it needs
  absent remote bytes, its typed error distinguishes uploading from missing
  and names the uploader. The app can ask for a missing file to be attached again.
- After a successful upload, reads on the attaching device use the cache
  or storage; coven never reads its user-provided original again.
  - The original stays where it is and is never changed or deleted.
- E.g. Ana attaches photo A to row 7, then Ben replaces it with photo B.
  Ana's upload of A finishes later. It changes no row and cannot restore A
  over B; each file keeps the reference chosen when it was attached.
- A row with no file has NULL in its hash and where-columns, so both
  must allow NULL; the app never writes them, and coven fills them in the
  write that attaches a file.
- A write that changes any of a row's file columns, its file, size, hash
  or where-column, writes all four, unchanged ones included, so a row's
  file always comes whole from one write.
  - E.g. Ana's phone and Ben's laptop each attach a different file to row
    7: the later write wins all four columns, never one file's size with
    the other's hash.
  - So coven refuses a foreign key with set null on any of the four, checked
    when the database opens and after migrating: its action would change
    one column alone ([§8.4](#84-foreign-keys)).
- An uploaded file is encrypted with a key of its own, which travels in
  its row's where-column, inside the row's encrypted writes, so only the
  row's readers can read it ([§16.2](#162-storage-and-naming)).
  - Moving a row between the store and a circle changes nothing about
    its file: the new audience's devices get the key with the row.

### 16.2 Storage and naming

- The content hash is a column of the row, and syncs inside encrypted
  writes like any other.
- Every device checks a downloaded file against it.
- Uploading stores the file encrypted at its already chosen path,
  `<store>/files/<device>/<file>`, using its attaching write's random key.
  - The provider sees only a random name, never a hash of the content.
  - Each upload is a copy of its own: identical files attached to two
    rows are stored twice, and deleting one never touches the other.
  - The key never reaches storage outside the row's encrypted writes, so
    a member who can't read the row can't read the file.
- A file is encrypted in 64-KiB chunks; the last may be shorter. Its
  header records the file size.
  - Each chunk is encrypted and authenticated on its own, with its index
    bound in, so a chunk can't be altered, swapped or reordered unnoticed.
  - So any chunk can be read and checked without the rest of the file.

### 16.3 Reading ranges

- The app reads a file through a *file reference*, taken from its row,
  naming the file the row had then.
  - A read checks the reference against the row first, so a row changed
    since can't redirect it to another file.
  - A write can check a reference the same way before changing the row.
- The app reads any byte range of a file, at any offset, as a stream.
- Playing a song and seeking in it are reads of different ranges.
- A file on this device, or in the cache, is read with positioned reads of
  the file on disk.
- An uploaded file not in the cache is read by fetching only the chunks that
  cover the range, with ranged requests to the provider.
  - Reuse its checked header across opens while it remains cached. If it
    is missing and the requested range starts at zero, fetch it with the
    first chunks; seeking elsewhere may need a separate header request.
  - Neighbouring chunks are fetched together, up to 1 MiB per request.
  - Each chunk is checked as it arrives; a failed check fails the read.
- Fetched chunks go into the cache, so reading a range again costs nothing.
- While a file is read in order, coven fetches the chunks ahead of the
  reader, so playback doesn't wait on the network.
- Seeking costs only the chunks under the new position.
- Reading a range whose chunks aren't cached, while offline, returns the
  storage network failure and its native cause for the app to show or log.
- Pinning a file ahead of time is how it becomes available offline.
- File reads, pinning and eager fill share verified cache ranges and one
  pending record per fixed file subject. An immediate call retains its typed
  cause; the list records the first unmet condition:
  - no connection is `NoStorage`; network and provider failures retain the
    provider's `Storage` category and retry temporary failures with §16.5's
    persisted delay;
  - confirmed absence with an active, capable uploader is
    `Waits(Object(path))`; removal, replacement or a reported source failure
    is `FileUnavailable(FileMissingReason)`, requiring app action;
  - a complete chunk failing authentication is `Refused(Decryption)`;
    authenticated bytes disagreeing with the row's content hash are
    `Refused(ContentHash)`. Neither retries the same immutable bytes
    automatically. §19.1's applicable update can permit a recheck;
  - a failed local cache write is `Failed(Disk)`; database failure is
    `Failed(Database)`. No failed range is marked cached or pinned.
- A file key travels inside its row. If the audience key needed to read that
  row has not arrived, the write or snapshot waits with `KeyUnavailable`
  before it can yield a `FileRef`; there is no invented file download with
  an unknown reference. Acquiring the key reuses the retained encrypted input.
- Ben's eager download has two verified ranges when Wi-Fi drops. The list
  reports the network failure and progress remains at the two committed
  ranges. Retry fetches the remainder. A bad final content hash instead
  leaves a permanent refusal and publishes no completed file or pin.
- The download verification boundary, including disk-full injection through
  the real Rust reader, is specified in [IO and observable work](proofs/io.md).
  It must check that bytes are authenticated before delivery and the complete
  sequential read checks its content hash before releasing its final buffer.

### 16.4 Cache

- Each device caches uploaded files and chunks of them, within a size budget
  the app sets.
  - When the cache is over budget, the least recently used files and chunks
    go first.
  - Freeing space never fails a read.
- The budget is per namespace, the group a table's declared files belong
  to ([E2](api.md#e2-declaring-synced-tables)); each namespace evicts on its
  own ([E8](api.md#e8-files-and-the-cache)).
- The app can pin a file to keep it whole on the device regardless of the
  budget, and unpin it.
  - Pinning assembles a complete encrypted cache file from cached and fetched
    chunks, checks its content hash, and syncs it before publishing the pin.
    Publication replaces the separate cached chunks atomically. Cancellation
    or a crash leaves no partially downloaded file exempt from eviction.
- The app can remove an uploaded file from the cache, which never touches
  storage.
- Eager-fill work and progress are derived from cache reservations and verified
  cached ranges. Reservations retain the requested fixed references and
  cancellation; they do not duplicate byte counters or failure reasons.
  Completed reservations stay in their requested set until that set finishes,
  then retire together. Evicting a completed fill does not request it again.
  A new changed file reference can request a new fill.
  - `eager_cache_fill_status()` and its subscription execute one query.
    Cancellation commits before it is reported. Failures remain in `pending()`.
  - `pin_progress(files)` and its subscription query verified cache coverage
    for the same references. Neither pinning nor eager fill reports bytes
    that have arrived but have not passed authentication and committed.
- Ana restarts after the provider confirmed two upload parts and the cache
  committed three download ranges. Her first reads and subscriptions show
  those same parts and ranges, even before storage reconnects. If open
  replaced her old device, both reset-notice reads return that stored notice.

### 16.5 Uploads and deletion

- Member-key replacement never closes a device's logs or file folder.
  Its send bound concerns old credentials only (§10). Retain files referenced
  by original inputs or dependent losses while a replacement decision can
  reverse; release needs its final prefix and all ordinary reference,
  snapshot-coverage and provider-cut-off checks. Per-change losses preserve
  their file references even when another change in the same write applies.

- The attaching write queues each file atomically with its row. It waits
  there until storage is connected and the upload succeeds.
- A separate file-upload worker services that queue outside sync passes.
  It wakes for queued work, changed prerequisites or a due retry, and shares
  the connection's identity check, transfer limits and backoff. It uses the
  file key fixed at attachment; it does not select an audience key.
  Pass completion does not wait for file uploads, and upload completion
  supplies retention evidence without scheduling another full pass.
- The queue retains the attaching write's fixed id, key, captured reference
  and source location, with its size, content hash, original modification
  time when applicable, and queue-owned chunk hashes. Retargeting the row
  cannot replace the source facts of an already queued file. It keeps no
  encrypted copy.
- Every attempt, including retries and resumed provider sessions, checks
  the source's size and whole-file content hash, and a user-provided
  original's recorded modification time, before sending file bytes.
  As it streams, it checks each plaintext chunk against its recorded hash
  before encrypting or sending that chunk. A missing or changed source
  fails the upload with the existing file error and records
  `FileUnavailable` for this device and file (§19.1). Its positions publish
  that typed reason; a differing chunk is never encrypted or sent.
  - A retry must encrypt the same plaintext under the same key and chunk
    nonces ([§11.1](#111-cryptography)). The per-chunk check guarantees
    this even if the source changes after the whole-file check, without
    keeping a copy. Size and modification time alone cannot guarantee it.
  - A provider part may begin or end inside an encrypted chunk. Coven
    reads and verifies the whole plaintext chunk before encrypting it
    and selecting the requested bytes.
- Automatic retry delays start at 1 second and double to at most 5 minutes.
  Before waiting, persist the not-before wall-clock deadline T and the full
  wait W with the work. During a running session a monotonic timer enforces
  the delay; wall-clock changes do not move that timer.
  - On reopen arm `max(0, min(T - now, W))` on the monotonic timer. Keep the
    original T and W until an attempt supplies a new delay; reopening does
    not reset the backoff exponent or replace the persisted deadline.
  - Operations, invites, joining, missing keys and file retries share this
    rule. Persist provider/account cooldowns separately at their actual
    scope before any affected worker can retry. `Retry-After` may make W
    exceed five minutes; neither restart, sync_now nor app retry discards it.
  - Ana restarts with four seconds left in an eight-second delay. She waits
    four seconds. If her clock moved backward, the restart waits at most
    eight; moving it forward can shorten the wait to zero. This rule bounds
    restart delays, not real-time cooldown duration under arbitrary clock jumps.
  - Failure to persist a delay fails the initiating work and prevents automatic
    retry; it is never converted into permission to retry immediately.
- A large file goes up through the provider's resumable or multipart
  upload, in parts.
  - Providers require it above a size, such as Google Drive above 5 MB per
    request.
  - The upload session records each provider-confirmed part and its byte count
    before reporting progress. Resuming and the app's `uploads()` /
    `subscribe_uploads()` pair read that same state. Bytes written to a socket
    are not progress; no separate progress writes or memory channel exist.
  - Upload pause state is also persisted. One-request uploads remain
    `Waiting` until confirmed completion removes them from the queue.
  - After a crash, an extant session continues from its confirmed parts;
    no confirmed part is forgotten merely because the app reopened.
  - A session the provider has since expired starts over, reading and
    verifying the source again with the same id, key and chunk hashes.
  - Any object past the provider's single request limit goes up this way,
    a large write or snapshot included.
- Upload completion removes the queue row and its pending record in one
  local transaction. It creates no app write, changes no row or file version,
  and does not delay the attaching write's upload.
  - A crash before that transaction retries the fixed upload. An occupied
    path counts only after its bytes compare equal (§10).
  - Rows may arrive before their files. No write waits for file bytes.
- Physical deletion of a circle's rows, files and objects, pre-migration
  inputs and local file sources also waits for every entry it depends on
  to be final (§9). Current replay alone cannot release them.
- A stored file is deleted only when no retained row, loss, snapshot or
  log write refers to its fixed path. Local rows, waiting writes, upload
  queues and inputs retained for non-final entries protect it too.
  - Check local protection first. If it protects every eligible known
    file, no snapshot rows or log parts need reading for file retention.
  - Otherwise stream the necessary retained data into the database's
    reference checks, with complete object authentication before deletion.
    Keep checked references and metadata by immutable object identity across
    passes and reopening, with an explicit completeness record. Loading and
    retention share this index; an object already checked is not downloaded
    again. Keep inspected ranges and any subsequently loaded body under the
    retained-input budget; checked references have no pass-end expiry.
  - Reconsider deletion when references, protected inputs, finality,
    ownership or file presence change. Discover files from their fixed row
    references, retained inputs and this device's upload queue; read or check
    status at the exact path. There is no idle file listing.
  - Under §13's storage cut-off assumption, the device responsible for a
    removed device's cleanup lists `<store>/files/<device>/` once after
    access is taken away. Cut-off must prevent in-flight sessions from
    publishing later too. This complete listing finds orphan files whose
    attaching rows did not survive; incomplete scans fail visibly and retry.
    Keep the discovered paths and scan completion so later passes and reopen
    continue the reference checks without listing again. If access is restored,
    a later cut-off needs its own scan. Removal alone does not authorize a scan
    to be treated as the last one while storage access remains unresolved.
  - Ana removes Ben while his last photo upload is in flight. After his
    access is cut off, she lists his file folder: the photo either landed
    before cut-off and is included, or cannot land. Retained references and
    finality still decide whether she can delete it.
  - When deletion needs remote reference completeness, list the relevant
    snapshot folders then and retain their checked coverage and references.
    This is due retention work, not an idle scan of snapshots or files.
- Its storage path and fixed row reference carry the uploader's device
  id, so ownership remains known after the last reference disappears.
- If retained data belongs to an unreadable audience, lacks a key copy,
  or fails validation,
  a device cannot prove file absence and leaves uploaded files in storage.
  Keep that uncertainty by object identity; retry the local proof only when
  its prerequisite or applicable reader version changes, or the object is
  deleted. Another pass alone does not download the same retained data.
- Uploaded files are deleted by the same devices as logs
  ([§15](#15-snapshots)).
- Deleting a row deletes only coven's copies of its file, never a
  user-provided original.

### 16.6 What a device keeps about files

- Coven keeps, in its local tables, what only this device knows about
  files; none of it syncs:
  - `_coven_user_files`: each user-provided file's path, size and
    modification time, by its row and column;
  - `_coven_device_files`: each app-provided file waiting to upload, and
    where in coven's own folder;
  - `_coven_file_chunks`: one plaintext hash per 64-KiB chunk, keyed by the
    fixed `(store, device, file id, chunk index)` reference. The upload queue
    owns these rows; attachment inserts them with the queue, and confirmed
    upload completion deletes them with it. Hashes use separate rows so
    file length does not become a single SQLite-value limit;
  - `_coven_file_uploads`: the upload queue, each file's captured reference,
    source location and original-source metadata, independent id and key,
    and its provider session while one is in progress;
    failures and waits use `_coven_pending`, not a second failure column;
    - Provider sessions belong to this queue; file upload operations do not
      keep a second recording of the same session.
    - Native error objects are available in the running process. Reopening
      exposes the persisted failure category rather than reconstructing an
      operating-system or provider error from text.
    - A stored file whose row changed is considered for storage deletion
      under the same retained-reference checks; no marking write is queued;
  - `_coven_cache`: each cached file or chunk, its namespace, verified range,
    size, when it was last read, and whether it is pinned. Cache reservations
    retain the fixed references requested for eager fill and cancellation,
    including requests that have not received any bytes;
  - `_coven_cache_budgets`: each namespace's budget and cached byte total,
    updated with its cache records so a budget check needs no full sum;
  - `_coven_file_removals`: unused local copies waiting to be deleted.
- Reading a not-yet-uploaded original finds its hashes through the row's
  fixed where-column reference. There is no row-and-column hash copy to keep
  in sync with the queue. After upload, reads use cache or storage (§16.1).
  - Ana attaches A, then the row is pointed at B before A finishes. A's
    queued reference still owns A's hashes; B's attachment records B's hashes
    under B's own id. Finishing A cannot delete B's facts.
- The bytes themselves are files in the store's directory: coven's own
  copies, and the cache.
- A file's bytes are written and synced to disk before the row that names
  them commits, and a row's removal commits before its bytes are deleted,
  so a crash never leaves a table naming bytes that aren't there.
- Nor bytes that no table names:
  - before writing new bytes, coven records their name in
    `_coven_file_removals`, and the write that attaches them takes it out;
  - the write that lets bytes go records them there in its transaction;
  - after a write commits or fails, and when the database opens, coven
    deletes the unused bytes `_coven_file_removals` names, then their records.
    Names still held by staging or cache reservations are retained. Cancelling
    either releases its active names synchronously, without requiring an async
    runtime. The durable removal records retain its abandoned bytes for the next
    write or open, which reports any deletion failure.
  - A deletion that fails stays recorded and is tried again then; the
    write that let the bytes go stays committed, and reports the failure.

## 17. Schema changes

### 17.1 Host application

- Every write records the schema version it was made with.
- Adding tables or columns is an *addition*.
  - Writes from devices on older versions still apply everywhere; they
    just don't set the new columns.
  - A device holds writes from a newer version until its app updates, then
    applies them.
  - E.g. Ana's app adds `color` to notes; Ben's older app keeps editing
    titles, and those edits reach Ana as usual.
- Any other change is a *breaking change*: renaming, retyping or dropping a
  column or table, or changing its constraints.
- Coven tells which a migration is by comparing the schema before and
  after it, as SQLite lists it.
  - Only new tables or new columns: an addition.
  - Anything else, such as a new index, constraint or foreign key: a
    breaking change. Changing an existing table's `owner_only` declaration
    is also breaking; compare declarations as well as SQLite's schema.
  - Changes only to local tables that keep their names, their columns,
    indexes and triggers, and to views, are neither: they don't sync.
  - Dropping or renaming a table is a breaking change, since it may have
    synced.
  - A migration that inserts, updates or deletes rows of synced tables is a
    breaking change, even if it only adds tables or columns: each device
    would otherwise compute those changes from its own rows.
- For each breaking change, the app supplies a *migration* in two parts:
  - one changes the database, e.g. `ALTER TABLE notes RENAME COLUMN title
    TO name`;
  - optionally, one converts an eligible row change of a received late
    old-version write, e.g. turns "note 43, title: X" into
    "note 43, name: X". It never changes an upload record.
- An update runs every pending migration in one transaction, then checks
  the schema rules against the tables the app declares, which describe
  only the newest version; if a rule fails, nothing changes.
  - E.g. migration 1 makes `notes(id)` and migration 2 renames `id` to
    `note_id`: only the final schema has to match `key_columns(["note_id"])`.
- A breaking change raises the store's schema version, which every device
  shares.
  - Whichever device's app updates first makes it, whoever's device it is.
  - That device runs the migration's first part on its database, writes a
    snapshot in the new version ([§15](#15-snapshots)), and records the store's new version
    in the store log ([§9](#9-members-and-roles)).
  - An older app cannot apply that audience's newer schema. It records
    `UpdateRequired` and reloads from the snapshot after updating.
    Store-log reads and independent work continue (§19.1).
  - It snapshots every audience it can read, with one entry raising each
    to the new version; the store's own entry raises the store.
  - For a circle it can't read, the first device of one of that circle's
    members to update writes the circle's snapshot in the new version,
    with an entry raising the circle to it; the circle's other devices
    reload from that one.
  - A device that has migrated queues ordinary new-version writes behind
    its migration marker and sends them only after replay keeps a raise
    to their version, its own or another device's
    ([`post_migration_behind_marker`](proofs/queue/CovenQueue/RaiseUpload.lean),
    [`new_schema_send_requires_kept`](proofs/queue/CovenQueue/RaiseProgress.lean)).
  - A device whose app is older than the store stops uploading; after
    updating, it sends its waiting old-schema records unchanged, including
    records never attempted before migration
    ([`old_schema_head_enabled`](proofs/queue/CovenQueue/RaiseUpload.lean),
    [`conversions_preserve_untried`](proofs/queue/CovenQueue/Conversion.lean)).
  - Raises to different versions don't conflict: both apply, the store is
    at the higher one, and devices reload from its snapshot.
- What a breaking migration changes in the synced tables is one write by
  the device that runs it, in the new version: its *migration write*.
  - It holds what differs between the synced tables before and after the
    migration: each row inserted or deleted, and each cell whose value
    changed.
  - A cell the migration left as it was keeps the write that set it, so a
    late write competes with that write, not with the migration.
  - A cell whose references change, such as one a new foreign key now
    covers, counts as changed: the migration write sets it, naming the
    parent's generation after the migration
    ([§8.4](#84-foreign-keys)).
  - Only the tables before and after count, not the statements between:
    a table or column after the migration is the one renamed to it with
    `ALTER TABLE … RENAME` from one that existed before, or else the one
    with its name.
    - E.g. `DROP TABLE b; ALTER TABLE a RENAME TO b`: the new `b` is the
      old `a`, and the old `b` is gone.
  - So a renamed column keeps its cells' writes under its new name, and so
    does a table rebuilt by copying it into a new one and renaming that
    back; a table or column with no match after the migration loses them.
  - E.g. Ana's migration renames `title` to `name` and fills a new column
    `slug` from it: each note's `name` keeps the write that last set its
    title, and each `slug` is set by the migration write.
  - Its object in the device log names it and holds no row changes: its
    changes reach other devices only in the breaking change's snapshot.
  - So a device that runs the same migration later, then reloads from that
    snapshot, changes nothing anywhere with its own migration write; nor
    does one whose breaking change loses to a concurrent one.
- An update that runs several breaking migrations makes one migration
  write, and raises the store once, to the newest version.
- A breaking migration deletes rows already hidden by removal rules through
  the ordinary generation path (§8.3).
  - Its migration write moves each such row to the next even generation.
    Keep `_coven_rows`; late edits at the old generation lose to that delete.
  - Capture one frozen whole-row loss with the written values and their
    setters, caused by this breaking version. A plain delete would retain
    only unread values, but the migration had read them all.
  - Use the same schema-excluded loss shape as an excluded write.
    The generation delete removes its live removed-row record.
  - Frozen values retain their old names and scalar values. References have
    no live parent links, even if the migration drops their foreign key.
- A non-final circle deletion cannot decide that a migration loses a row.
  - Leave out rows whose only removal cause is that `DeletedCircle` rule.
    Keep their values and merge records for replay.
  - Judge independent causes with non-final deleted-circle causes absent;
    a child hidden solely through such a parent is left out too.
  - A row also failing an independent CHECK or other removal rule still
    follows the generation-delete and frozen-loss rule.
  - E.g. Gifts' deletion hides Ana's note 7. Ben's app migrates while that
    deletion is non-final. The migration retains note 7's inputs; if the
    deletion drops, note 7 returns through the winning migrated state.
  - If note 8 independently fails `start <= end`, the migration deletes
    its generation and keeps one frozen loss even if Gifts returns.
- Pre-migration inputs remain until the deciding entries are final (§9).
  If a deletion changes before then, recompute the migration's derived
  state from those inputs; do not revive erased cells by guessing values.
- Dropping a table or column does not dismiss its pending losses.
  - Freeze each affected loss as old-shape data before removing its schema
    records. A whole-row loss keeps the whole captured row.
  - Keep its table and column names, key, audience, scalar values, setters
    and original cause. Strip live parent links; the old schema need not exist.
  - Frozen records no longer take part in merge or current removal rules.
    They remain readable, dismissible, snapshotted and fingerprinted.
  - E.g. Ana's migration drops `notes.color` while Ben's losing value “blue”
    is still pending. Every device keeps that loss under `notes.color`;
    opening the new schema does not silently discard it.
- Migration preserves all queued original records, tried or untried:
  device, number, timestamp, causal positions, schema, disposition and
  plaintext remain unchanged; a tried record also keeps its format and
  sealing keys
  ([`conversions_preserve_uploads`, `conversions_preserve_attempt`, `conversions_preserve_untried`](proofs/queue/CovenQueue/Conversion.lean)).
- Upload those old-schema records in number order, even while the updated
  app's schema is ahead of the kept store version, with the ordinary key,
  identity and access gates still required
  ([`old_schema_head_enabled`](proofs/queue/CovenQueue/RaiseUpload.lean),
  [`prefix_uploads`](proofs/queue/CovenQueue/RaiseProgress.lean)).
  - A positive upload result or complete equal-byte comparison settles
    the original record; keep its queue record and pending reason until
    confirmation, regardless of local snapshot capture
    ([`confirmation_requires_equal`](proofs/queue/CovenQueue/Upload.lean)).
- The migration appends its empty log marker after the existing queue and
  captures the raise snapshot through that marker, excluding subsequent
  app writes
  ([`migration_preserves_waiting`, `post_migration_behind_marker`](proofs/queue/CovenQueue/RaiseUpload.lean)).
  - The marker is the only new-schema record allowed to upload before a
    kept raise; its row changes travel only in the snapshot
    ([`migration_marker_enabled`](proofs/queue/CovenQueue/RaiseUpload.lean),
    [`old_prefix_and_marker_enabled`](proofs/queue/CovenQueue/RaiseProgress.lean)).
  - Confirm the captured own prefix, including the marker, before sending
    the snapshot under §15's publication gates, then publish the raise
    entry after that snapshot is stored; every covered remote position
    also requires retained publication evidence
    ([`publication_after_confirmation`, `published_raise_covers_published`, `entry_has_snapshot`](proofs/queue/CovenQueue/RaiseUpload.lean),
    [`inherited_position_published`](proofs/io/CovenIO/SnapshotWrites.lean)).
  - No empty upload queue is required: later app writes wait behind the
    raise without delaying that captured prefix's publication
    ([`no_raise_deadlock`](proofs/queue/CovenQueue/RaiseProgress.lean)).
  - E.g. Ana queues old-schema write 1, migrates with marker 2, then makes
    new-schema write 3: confirm 1 and 2, publish the snapshot and raise,
    and send 3 once a raise is kept
    ([`raise_publishes_with_new_tail`](proofs/queue/CovenQueue/RaiseExamples.lean)).
- Conversion is for received late old-schema writes from other devices,
  including already-attempted writes that arrive after the raise. After a
  competing raise wins, the original write's author uses that same receive
  rule to rebuild local state from the unchanged records
  ([`reload_matches_receiver`, `conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
  - Let S be the kept breaking-change snapshot for the audience: S supplies
    covered effects, with consumed positions and retained source verdicts
    kept separately (§8). Eligible uncovered ordinary old-schema changes
    use the winning raise's supplied conversions in version and causal
    order, independently of whether their upload was tried
    ([`receive_matches_evaluation`, `receive_agreement`](proofs/queue/CovenQueue/Conversion.lean)).
  - Member-key window exclusions supply base verdicts to the same §8 data
    rule (§10); conversion cannot rescue them.
  - First determine candidate schema admission without changing values,
    check owner permission using the current table declaration and retained
    original identities (§8.8), and verify foreign-key effects against the
    write's original schema. Resolve each change's original data sources
    under §8. Invoke the
    callback only on eligible changes. A dependent change stays frozen in
    its original shape with its failed sources as the cause; unrelated
    changes in the same write, even the same part, can convert and apply
    ([`dependent_change_not_converted`, `independent_change_converted`, `converted_change_matches_receiver`](proofs/queue/CovenQueue/Conversion.lean)).
  - Without a converter, exclude the old-version interpretation: retain
    each original change as a frozen loss naming the breaking version,
    including a delete with no scalar values, and pass the position.
    Already frozen changes keep their original values and cause through
    later raises; no callback rewrites any tried or untried upload
    ([`missing_converter_preserves_values`, `conversions_preserve_frozen`, `lost_never_reconverted`](proofs/queue/CovenQueue/Conversion.lean)).
  - Recorded reads, including implicit own predecessors, govern receive
    order, not exclusion. No adoption-exclusion list is needed. Keep the
    selected raise/reset, snapshot coverage and `store_log_read` for their
    own boundary checks, and retain per-change source verdicts independently
    of dismissible losses. A later snapshot covering an excluded position
    does not turn its dropped data into a source
    ([`recorded_reads_do_not_exclude`](proofs/queue/CovenQueue/Model.lean),
    [`snapshot_drop_stays_dropped`, `snapshot_loss_preserves_cause`](proofs/queue/CovenQueue/Verdicts.lean)).
  - Reset-ignored parts follow §19.3 before schema exclusion; they and
    losing migration markers create no schema loss. Otherwise eligible
    changes built on a reset-ignored source follow §8's data rule.
  - Commit interpreted changes, frozen originals, source verdicts, merged
    rows and consumed positions together. A failed conversion or reload
    preserves the preceding committed state and reports the failure.
- E.g. Ana's kept snapshot covers Ben through 8. His already-attempted
  old-schema write 9 arrives after the raise and creates a note. With the
  supplied converter, every device converts it on receipt, including Ben
  after a competing raise; the upload's original bytes stay unchanged
  ([`concurrent_raise_old_write`](proofs/queue/CovenQueue/RaiseExamples.lean)).
  - If 9 has already been excluded without a converter, a later callback
    cannot revive it. Ben's old-schema 10 edits that note and renames an
    unrelated playlist whose generation and replaced setter survive.
    With a converter for 10, its note edit is a frozen dependency loss and
    its playlist rename converts and applies on every device
    ([`mixed_write`, `own_reads_do_not_exclude`](proofs/queue/CovenQueue/DataExamples.lean)).
  - The same holds if the note's generation survived but the cell setter
    that 10 replaced came from excluded 9. Later changes built on 10's
    playlist rename can apply; changes built on its frozen note edit lose
    ([`actual_setter_decides`, `partial_write_chain`](proofs/queue/CovenQueue/DataExamples.lean)).
  - Without a converter for either write, both keep their original changes
    as schema losses. Ben's write 11 after the atomic reload uses the
    winning state; reading the passed positions of 9 and 10 alone does not
    exclude it.
- Loading a winning snapshot replaces the local values computed by a losing
  migration; those values become no shared losses, while ordinary excluded
  writes retain their original-value losses
  ([`competing_raise_converges`](proofs/queue/CovenQueue/RaiseExamples.lean),
  [`receive_losses_converge`](proofs/queue/CovenQueue/Conversion.lean)).
- If two devices make the same breaking change at once, the one with the
  smaller timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

### 17.2 Coven's schema

- Coven's own tables, indexes, triggers and views, including temporary
  objects, use the `_coven_` prefix. SQLite-generated constraint indexes
  keep SQLite's `sqlite_autoindex_` prefix before the table name.
- App SQL objects and synced-table declarations may not use `_coven_` or
  `coven_`, compared without regard to ASCII case. Reserving both prefixes
  keeps app access restricted to app objects; coven creates its own objects
  only under `_coven_`. SQL functions such as `coven_applying()` keep their
  names.
- Coven's own tables in the local database, such as `_coven_rows`, are
  local only. Opening for writing always migrates them in place before
  running the app's migrations; each internal migration is atomic.
  A read-only open refuses tables that need migrating, and any open refuses
  an internal schema newer than this coven supports.
- What coven writes to storage has a *format*: write records, store log
  entries, snapshots, paths, every byte of it given in
  [Appendix D](format.md).
- Every object records the format version it was written in, outside its
  encryption, so an older coven tells a newer object from a damaged one,
  and asks for an update instead of reporting it.
- Format 1 is the first, unreleased format. Its layouts include stable
  member ids, replacement entries and credential-specific copies/codes (D1).
  There are no older formats or compatibility readers. After release a layout
  change gets a new format number: a newer coven reads every released older
  format and writes the newest, since a device can return with waiting writes
  from any released format.
- A write or store log entry already tried is retried in the format of
  its first attempt, so re-sealing it reproduces the same bytes under the
  same content-bound nonces ([§6](#6-syncing-writes)).
- An older coven that encounters an object in a newer format asks for an
  update and waits, as with schema additions. Once updated, it reads that
  object and continues.
- Format versions belong to objects. A format change needs no store-log
  entry, snapshot or reload.

## 18. Operations

- An *operation* is work that takes several steps, any of which a crash can
  interrupt, such as publishing a rotation.
- Coven keeps every unfinished operation in one local table:

  ```sql
  CREATE TABLE _coven_operations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,     -- 'rotate key', 'reload from snapshot', …
    last_step   INTEGER NOT NULL,  -- 0 before the first step completes
    data        BLOB NOT NULL,     -- what this kind's steps need, in its own shape
    started_by  TEXT NOT NULL      -- the app call that started it, or 'coven'
  );
  ```

- Operation ids are never reused, so a retained app reference cannot retry or
  discard another operation after its original row is deleted.
- Each kind defines its own steps and the shape of its `data`, so one table
  holds any operation:

  ```
  id   kind                   last_step   data                                started_by
  1    rotate key             2           audience: store, key: …             coven
  2    reload from snapshot   1           snapshot: <store>/snapshots/store/ana-phone/7,    coven
                                          temporary file: …
  ```

- A step that changes the local database updates `last_step` in the same
  transaction:

  ```
  BEGIN
    ALTER TABLE notes RENAME COLUMN title TO name
    UPDATE _coven_operations SET last_step = 1 WHERE id = 3
  COMMIT
  ```

  - A crash before the commit undoes both, so the step runs again.
  - The migration can never be done while the row says it isn't, which
    would run it twice.
- A step that changes storage is safe to run twice.
  - Within one recorded attempt, writing an object writes the same path
    with the same bytes, including after a crash: writes and entries
    regenerate them from fixed plaintext and key ids; sealed keys and
    snapshots retain fixed bytes ([§6](#6-syncing-writes);
    [queue proof: `Operations.fixed_data`, `publication_retries_identical`](proofs/queue.md#what-is-proved)).
  - Deleting an object that is already gone succeeds.
- A store log entry takes its number when its plaintext and key id commit.
  While its signing credential remains current it is uploaded even if the
  operation is abandoned; replay judges it like any entry, since the device's
  later entries had read it. Replacement blocks old-credential sends (§10);
  it cannot skip a queued number or rewrite the entry.
- An operation that writes a store log entry finishes only once the replay
  keeps it; if the replay drops it, the operation starts over from its
  first step, against the member list it then has.
  - A member-key replacement is an exception: a losing attempt reports its
    replay reason and does not restart with a new entry to overrule the
    winner. Replacing again requires a new explicit call or approved invite.
    Publication retries of the same attempt retain its exact keys and bytes.
  - Unless the drop leaves nothing to do, such as removing a member a
    concurrent entry already removed.
  - Starting over repeats §9's online catch-up and validation. This also
    applies after “landed too late”: the old entry remains dropped and
    reported, and any replacement has a new number and recorded past.
    A failed validation reaches the caller and blocks the operation.
  - A rotation starting over makes a fresh key and copies with a new entry
    number. The old copies and entry remain in storage; the guarantee about
    fixed data applies separately to each recorded attempt
    ([checked history: `restart_changes_key`](proofs/queue.md#checked-histories-and-limits)).
- A device runs one operation that writes store log entries at a time, and
  none while it reloads from a snapshot.
- An app call first satisfies the online checks of §9. Once its operation
  starts, the call returns when it finishes or cannot proceed without app
  action. Dropping the call does not stop the recorded operation.
- Work already recorded can wait for storage to return. This does not
  allow an offline call to start a new administrative action.
- A recorded rotation or snapshot publication attempt finishes when its
  prerequisites hold and progress is fair: local persistence and checks,
  journal transactions, storage acceptance, confirmation, and final
  verification or kept-entry completion eventually succeed
  ([queue proof: `Operations.Fair`, `rotation_resumes_and_finishes`, `snapshot_resumes_and_finishes`](proofs/queue.md#what-is-proved)).
  Storage accepting every object does not suffice if replay drops the entry
  ([checked history: `publication_without_kept_entry`](proofs/queue.md#checked-histories-and-limits));
  permanent rejection still needs the applicable update or app action (§19.1).
- Other devices can see stored key copies before their introducing entry,
  and a complete snapshot before its publisher records verification
  ([checked histories: `copies_visible_before_finish`, `snapshot_visible_before_finish`](proofs/queue.md#checked-histories-and-limits)).
  The guarantees are:
  - an entry is uploaded after everything it refers to; in particular, a
    key introduction is stored only after all its required sealed copies
    ([queue proof: `rotation_entry_after_copies`, `visible_rotation_has_copies`](proofs/queue.md#what-is-proved));
  - a snapshot counts only when complete and passing the reader's checks
    (§15; [queue proof: `snapshot_stored_checked`](proofs/queue.md#what-is-proved));
  - a received write's changes and consumed position become visible in one
    transaction (§6;
    [queue proof: `AtomicReceive.never_half_visible`](proofs/queue.md#what-is-proved)).
- When the app starts, coven resumes every unfinished operation from the
  step after its last completed one in the recorded attempt. Replay drops
  follow the restart rule above.
  - Thereafter it wakes for commands, changed prerequisites or due retry
    timers, following [the pass's waiting rules](sync-pass.md#waiting-without-repeated-work).
    It does not scan and replay the store log every second while waiting.
  - Determine snapshot growth from local indexed facts before creating a
    snapshot operation. Retention derives eligible deletions each pass and
    has no operation row; an unchanged pending reason causes no durable rewrite.
- A step that cannot advance records its first unmet condition in `_coven_pending`
  in the same transaction that records what the step completed.
  - A waiting app call receives the typed error. The record retains its
    category across restart without converting an error into text.
  - Every operation is visible there, including snapshots, internal reloads
    and pending provider access work. Retention uses its own path subject.
  - The reason determines retry: automatic, after an update, app action,
    or never (§19.1, E5). Unrelated operations can still advance.
  - The app can retry or discard its own pending operations with E6.
    It can also retry failed provider access work. It cannot discard that
    current intention or automatic maintenance through those calls.
  - A failed reload leaves the old database in place. No positions pass
    it until the replacement and its dependent work commit.
- An operation's row is deleted when its last step completes.

### 18.1 Operations

- Removing a member ([§13](#13-removing-members-and-devices)) publishes one
  entry and returns once it is kept. Applying replay records access work;
  the removal itself has no provider step or operation row.
- Removing someone from a circle ([§14.6](#146-leaving-a-circle)) publishes
  one entry naming the circle and member. It is not a multi-step operation.
- Rotating an audience key ([§11](#11-keys)):
  1. a current member makes the key and id, records its SHA-256 commitment,
     and persists the key in custody;
  2. upload fixed sealed copies to the current audience members;
  3. upload the rotation entry with the audience, key id and key hash.
- A breaking schema change ([§17](#17-schema-changes)):
  1. migrate the database and append its empty migration marker in one
     transaction, preserving all earlier upload records, and capture through
     the marker before subsequent app writes
     ([`migration_preserves_waiting`, `post_migration_behind_marker`](proofs/queue/CovenQueue/RaiseUpload.lean));
  2. upload and confirm that old prefix and marker with their original
     records, then send the captured new-version snapshot only when §15's
     catch-up, coverage and publication gates hold
     ([`prefix_uploads`](proofs/queue/CovenQueue/RaiseProgress.lean),
     [`snapshot_covers_published`](proofs/io/CovenIO/SnapshotWrites.lean));
  3. publish the raise entry after its snapshot is stored; ordinary
     new-schema writes wait for a kept raise, without extending the capture
     ([`entry_has_snapshot`](proofs/queue/CovenQueue/RaiseUpload.lean),
     [`new_schema_send_requires_kept`, `no_raise_deadlock`](proofs/queue/CovenQueue/RaiseProgress.lean)).
- Reloading from a snapshot ([§15](#15-snapshots)):
  1. settle attempted uploads under §17.1, freeze the required positions,
     and stage all readable audiences through §15's common target,
     retaining the selected snapshots and checked write inputs; a certified
     deleted number replaces only its affected audience and recomputes the
     target for all audiences
     ([`committed_safe`](proofs/io/CovenIO/CommonReload.lean));
  2. replace the synced tables and coven's merge tables
     ([§8](#8-merge)) with the snapshots, apply those writes, and re-apply
     eligible waiting writes, in one transaction. Interpret late original
     records and their losses under §17.1's receive rule without changing
     any upload record
     ([`reload_matches_receiver`, `conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean));
     reset-ignored writes follow §19.3. Positions advance only with this
     realized state.
- Writing a snapshot ([§15](#15-snapshots)):
  1. write it, sealed, to a temporary file, record it and check it locally
     against the captured database state;
  2. upload it and compare the stored bytes' checksum. This ends the operation.
- Retention is one derived pass step, not an operation. It computes eligible
  log, snapshot and file deletions from verified coverage, finality, reader
  positions, ownership and retained references (§15, §16.5). Delete is
  idempotent. A failure has a `Retention { path }` record; no journal step
  duplicates eligibility or needs resuming.
  - Ana's snapshot is confirmed before she closes the app. Its publication
    is complete even though an absent reader still retains write 9. A later
    pass deletes 9 once the reader or storage-age condition permits it.
- Making a circle ([§14.3](#143-circles)):
  1. make its first key and its id, and record them;
  2. upload the key sealed to this member;
  3. upload the entry making the circle.
- Adding someone to a circle: seal each of its keys to them, then upload
  the entry adding them.
- Deleting a circle ([§14.7](#147-deleting-a-circle)) publishes its entry.
  There is no preceding write or wait for row uploads.
- Inviting a person ([§12.2](#122-adding-a-person)):
  1. share the storage with their account, or record the S3 key the admin
     made in the console, or make the key with admin access (§4: the attempt
     and principal name are recorded first, then the key, then its
     member-side check), and record the invite's chosen stable id. A failed
     mint rolls back; cancelling before the key exists rolls back with admin
     access;
  2. once an admin approves the exact id and new public pair, confirm the
     initial and held historical audience-key copies to that credential,
     then publish Add member or Replace member keys as the invite specifies;
  3. bootstrap waits for the kept matching binding before registering the
     device and recording any separate access change. A losing replacement
     reports rejection; it does not restart against the winning pair;
  4. on decline or expiry, take back only the invite's new access instead.
- Self-replacement of member keys (§10, §11) pauses local app writes and
  confirms queued writes/entries first. Persist the candidate private pair,
  publish its required key copies, then its replacement signed by the current
  key. After the binding is kept, refresh custody and the restore code before
  resuming app writes. Publication or custody failure retains the attempt and
  both pairs, reports its cause and resumes that same attempt on retry. Replay
  rejection reports its reason without creating another replacement.
  Discarding work cannot clear the
  pause while a reserved replacement still needs settlement or the current
  binding needs custody installed. Before any replacement entry is reserved,
  cancellation may release the pause only if held keys still match replay.
  Other installations keep their ids but cannot send with the old credentials.
- Uploading a write or a file is not an operation: it waits in its queue
  until stored ([§6](#6-syncing-writes), [§16.5](#165-uploads-and-deletion)).
  Writes are sent by the pass; files by the separate upload worker.
  - A large file's provider session is recorded in its queue row, with the
    last part stored, so after a crash it continues from there
    ([§16.6](#166-what-a-device-keeps-about-files)).

Ana's historical K1 copy for Ben has no `before_entry`. Her new K2 copy
for Carol names rotation entry Ana/9, so that entry cannot be published until
the copy is confirmed. Both use the same queue and the same byte-preserving
retry rule.

### 18.2 Example

- Carol rotates the store key after Ana removes Ben. Her phone crashes
  after uploading K2's sealed copies:

  ```
  _coven_operations
    kind         last step   data                          started by
    rotate key   2           store, K2, key hash            coven

  storage
    <store>/keys/carol-phone/17                uploaded: store, K2, Ana
    <store>/keys/carol-phone/18                uploaded: store, K2, Carol
    <store>/store-log/carol-phone/9             not yet: the rotation
  ```

- The copies cannot authorize K2 for sending until its introduction is read.
- After this crash Carol resumes the same attempt at step 3 with the same
  key, hash and sealed bytes
  ([queue proof: `Operations.fixed_data`](proofs/queue.md#what-is-proved)).
  A replay drop instead follows §18's restart rule, making a fresh key and
  entry while keeping the old objects in storage
  ([checked history: `restart_changes_key`](proofs/queue.md#checked-histories-and-limits)).
## 19. Recovery

- Recovery is for any state coven's rules didn't produce: a write that
  never arrives, an object that is damaged, a database a disk or a bug
  damaged, or devices holding different data after the same writes.
- The last is the one nothing else would catch, since every rule would
  still run; it is what a bug in coven or the app looks like.

### 19.1 Noticing

Old-key window drops use the write's `Dropped(LandedTooLate)` record; otherwise
admitted changes depending on dropped data appear as per-change losses (§8).
Until the kept replacement and its preceding prefix are final, a replay change
rebuilds admission, source evidence, rows, losses and pending records atomically.
No later reader rescues an excluded object. Old-credential send failures use
`MemberKeysReplaced`, independently of device removal. A key-copy subject names
the exact sealing credential as well as member, audience and key (D8); a report
for the old credential cannot satisfy a new-credential wait. Historical
snapshot, positions and clock signatures follow D9's separate object policies.

Ana is offline with a photo waiting to upload. Ben's key has not arrived.
Both appear in `pending()` with their own first unmet condition; the list also
holds failures and rejected entries, without making either wait look completed.

- `_coven_pending` holds one record per subject and reporting device.
  The app reads it with `pending()` or `subscribe_pending()` (E5).
  - The subject identifies the write, entry, key copy, snapshot, positions,
    file, operation, retention target, audience, join request or connection.
  - An agreement check names both its audience and the peer.
  - The reason is the first unmet condition in that subject's processing
    order. Do not duplicate one subject for all of its downstream symptoms.
  - Observe a block and save its record in one transaction. Replace it if
    the first condition changes; delete it when the subject advances or
    replay removes the need for that work.
  - Positions that move past a waiting write without applying it, as a
    loaded snapshot's do, end this device's wait for that write in the same
    transaction: the download never fetches a covered write again. A
    refusal or drop of the write's bytes stays
    ([`cover_safe`, `cover_keeps_verdicts`](proofs/storelog-data/CovenStorelogData/Pending.lean)).
  - A completed listing replaces this device's records for its subjects as
    a whole: each pass's positions scan decides every peer's positions
    record, and each audience's snapshot listing decides that audience's
    snapshot records. A subject the listing does not name has no object to
    process and loses its record; a failed listing changes nothing. This
    device's own positions record clears only when its publication is
    confirmed
    ([`listing_decides`, `listing_safe`, `own_positions_kept`](proofs/storelog-data/CovenStorelogData/Pending.lean)).
  - Each store-log scan decides this device's entry waits as a whole. An
    entry it reads that has not applied records its first unmet condition:
    a missing store key, a newer format, or the first entry of its recorded
    past that has not applied, checking its own previous entry first and
    then each entry it read. An entry behind a stopped entry of its own log
    has no record of its own. A wait the scan no longer finds ends;
    refusals and drops stay
    ([`entry_scan_decides`, `entry_scan_safe`, `entry_scan_removes_only_ended`, `entry_scan_keeps_verdicts`](proofs/storelog-data/CovenStorelogData/Pending.lean),
    [`first_missing_first_unmet`, `scan_names_only`, `behind_stop_never_named`](proofs/storelog-data/CovenStorelogData/PendingScans.lean)).
  - No wall-clock time or attempt counter controls these records. They
    describe current work, not a history of attempts.
- Record every wait and failure, including:
  - a gap in either kind of log, a listed object that cannot yet be read,
    an unknown author's device registration, or missing causal history;
  - a missing or damaged sealed key copy;
  - an app schema or object format that needs an update;
  - an unpublished schema raise, pending own uploads or a required reload;
  - a snapshot that cannot be used, a positions object that cannot be
    trusted, or history that prevents proving a file safe to delete;
  - physical cleanup waiting for an entry to become final, with
    `Waits(EntryFinality(entry))` naming that entry (§9);
  - file sources that cannot be uploaded, paused transfers, and provider
    work waiting for the owner, a request, or an app action;
  - a dropped entry, including “landed too late” on its author's device,
    a write that missed its device-retirement or member-key deadline (§10), or a fingerprint
    disagreement.
- E.g. Carol's tablet has Ben's write 9, which read Ana's write 4. GETting
  Ana's next write, 4, returns a miss.
  - Ana's next log position reports the missing object.
  - Ben's write reports that same object as its first prerequisite.
  - Carol keeps applying independent writes. Both records disappear when
    Ana's write arrives and those subjects advance.
- The typed reason determines retry (E5):
  - missing objects, prerequisites, key copies and transient storage work
    retry automatically;
  - an immutable object's permanent refusal, or an unsupported schema or
    format, waits for the applicable update;
  - a missing file source, retained provider grant or disagreement needs
    an app action;
  - replay drops are recomputed on replay, not downloaded repeatedly;
    an entry's “landed too late” drop never becomes eligible again. It
    passes the entry's position and remains reported even if a new entry
    succeeds. A reset can clear the report, but cannot undo that drop.
    A write's retirement deadline is instead replay-dependent (§10):
    dropping the retirement can restore the write without downloading it again.
- A permanent refusal requires a complete download and a failed check:
  decryption, authentication, signature, parsing, authorization, identity,
  causality or merge validation. Calls and pending records use the same
  `Refusal` type, including `InvalidCausality` and `WrongIdentity`; the
  failing call retains every native check cause in an optional field using
  the narrowest native error type. A check with no native error supplies no
  invented cause; variants whose checks never produce one stay unit variants.
  Persisted and signed reports keep the same variant without the cause;
  equality and D8 encode only that variant, never a process-local error.
  - Ben's write has a timestamp no later than Ana's write it read. Reading
    it returns `DamagedObject` with `Refusal::InvalidCausality` carrying
    `Some(Arc<MergeError>)`; its persisted pending reason is
    `Refused(InvalidCausality { cause: None })`. Neither path turns
    that permanent check into a network failure.
  - A refused write or entry stops its own log at that number. Later writes
    in that log and dependent work cannot pass it; independent logs continue.
  - Keep the coven package version of the local refusal internally.
    A different installed version permits one new attempt. Reopening the
    same version does not retry the same immutable bytes. A permitted
    check uses retained bytes first; only eviction requires another download.
  - A successful changed reset clears the current pending list atomically
    with its reload. A failed reload changes nothing. Retain suppressed
    records until the reset is final; if the reset drops, recompute the list
    from those inputs (§9). Still-unmet conditions are recorded when
    observed again; clearing applies even when a refused object's audience
    is unknown, to both kinds of log.
  - Snapshot loading alone does not clear refusals or bypass required
    store-log history.
- Posted positions carry the subset peers can act on (D8): permanent
  refusals, missing objects and prerequisites, unavailable member key copies,
  update requirements, and this device's undeliverable files.
  - Publish only observations made here, never reports received from peers.
  - Authenticate each report. Retain it when it names this device's object,
    an object needed from this device, a key copy sealed to this member,
    or an undeliverable file referenced by this device's retained data.
  - Peer reports inform the author or file reader; they do not dictate its own download
    checks. One completed positions scan replaces the received reports
    atomically. A failed scan leaves the previous reports and records why.
    The pass reuses peer objects whose listed identity is unchanged (§3.1,
    §4); agreement and retention use that same decoded state. An equal
    received report set needs no replacement transaction.
  - A device can publish these records while its write positions wait (§6).
    Silence or different positions alone never proves an immutable refusal.
- A damaged snapshot is recorded and passed over for the next latest, or
  the logs. Its verified prefix still establishes required history; it
  never authorizes applying a damaged body.
  - Each listing keeps this device's refusal of a snapshot it still lists,
    and loading never offers a refused snapshot again: it applies only a
    snapshot whose complete body checked. Only a different installed
    version permits one more check
    ([`listing_keeps_refusal`](proofs/storelog-data/CovenStorelogData/Pending.lean),
    [`never_loads_refused`, `refused_stays_unoffered`](proofs/storelog-data/CovenStorelogData/PendingScans.lean)).
  - Unverified prefixes establish no coverage.
  - Missing required logs block the reload without changing the database.
- A damaged positions object has reason `InvalidPositions` and counts as
  not posted. Its discovery retries automatically because its author can
  replace it; only a changed object identity or a permitted local recheck
  triggers another validation, using retained bytes when available.
- A damaged local database is detected by SQLite's integrity check or
  decoding its stored facts (§19.2).
  - Internal store-log failures—an invalid checked entry, a replay set that
    changed, or changed bytes/checks for an already stored entry—cross the
    app boundary as `Failed(LocalFailure::Database)`. The initiating call
    retains its native cause through `SyncError::Failed`; crate-to-crate
    error variants are not app-facing `DbError` variants.
  - Ben's replay returns a set missing a received entry. The transaction
    fails, his entry remains pending with `Failed(Database)`, and positions
    do not advance. The app sees a database failure, not an action to repair
    an internal entry handoff.
- Devices that disagree:
  - each device keeps a *fingerprint* of the data in each audience it can
    read, updated as writes apply;
  - it is a hash of exactly what every device must agree on:
    - the rows the app sees;
    - each row's generations;
    - which write set each cell, named by device and number;
    - its `_coven_lost` rows, with their values as written;
  - all computed as if the rule for a key present in two audiences didn't
    exist, since which row it shows depends on which circles a device
    reads ([§14.2](#142-moving-rows));
  - and nothing else that differs between devices by design, such as
    `_coven_uploads`, `_coven_operations`, or local row ids;
  - it is keyed with a key derived from the store's or circle's key, so the
    provider learns nothing from it;
  - each device posts its fingerprints with its positions ([§6](#6-syncing-writes));
  - two devices that have applied exactly the same writes must have the
    same fingerprints;
  - so whenever two devices are at the same positions, in the device logs
    and the store log, and on the same schema version, as they usually are
    once a store is quiet, each compares, and a mismatch means a bug made
    one of them wrong;
  - on different versions they can't compare: an added column exists on
    one device only.
- A fingerprint mismatch is an `Agreement` pending subject naming the
  audience and peer, with reason `Disagrees`.
  - Compare only fingerprints using the same key as well as the same
    write positions, store-log positions and schema version.
  - E.g. Ana and Ben reach the same positions in Gifts but their hashes
    differ. Each app sees the peer and Gifts in its pending list.
  - Neither device can tell which copy is right. Recovery requires the
    person's choice of reload (§19.2) or reset (§19.3).

### 19.2 Recovering one device

- A usable database that disagrees with another device reloads in place
  from snapshots and logs (§15). It keeps its device id and waiting writes.
  - Settle attempted writes by sending their original bytes. Convert
    eligible received old-schema records or record their original-value
    losses under §17.1, leaving upload records unchanged
    ([`reload_matches_receiver`, `conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
  - Reset boundaries still decide which parts apply (§19.3).
- A damaged database fails to open with `DbError::DamagedDatabase`.
  The app calls `CovenBuilder::reset_device` (E1).
  - Use the same new-device path as a stale copy (§10), from saved settings
    and custody. Read no rows, counters, queues or operations from the
    damaged database for salvage.
  - Check storage and unlock the member and store keys before moving the
    damaged files. A missing setting, unavailable key or failed connection
    leaves them in place and returns its typed error.
  - Stop all local database use and hold the store's writer and reader locks.
    Move the old directory aside as a whole, including SQLite journals and
    local file sources. Keep it available for inspection, not as live state.
  - Use the existing bootstrap state (§12) to keep the new id, replacement
    entry and old directory identity before moving it. Resume that bootstrap
    after a failure; ordinary opens refuse the unfinished installation.
    There is no separate recovery journal or recovery marker.
  - Load storage under the fresh id, then publish the replacement handle.
    Never import unsent rows or files from the damaged directory.
  - Before publishing that handle, commit its `_coven_device_reset` record
    with the old and new ids and `DatabaseDamage` (E5). Both notice queries
    report that unsent edits may have been lost, including after reopening.
    Failure to store the notice fails replacement; it cannot publish a usable
    handle that conceals the discarded installation.
- E.g. Ana's laptop has uploaded through write 12, but its damaged database
  may contain write 13. Resetting loads the stored history through 12 under
  a new device id. Coven does not guess which fragments of 13 are usable;
  the app receives the loss notice.

### 19.3 Resetting a store

- When the problem is shared, or nobody can tell which device is right,
  the person picks the device they trust, and the store is reset from it.
- E.g. "Ana's phone and Ben's laptop disagree about the store; reset it
  from this device?"
- An admin's device resets the store's rows; a member of a circle resets
  that circle's rows, one reset per audience.
- A reset is an operation ([§18](#18-operations)):
  1. write a snapshot of what this device has ([§15](#15-snapshots));
  2. record in the store log that the store, or the circle, is reset to
     that snapshot ([§9](#9-members-and-roles)).
- Every device reloads from that snapshot when it applies the entry, the
  resetting device too, so writes it applied after writing the snapshot
  are judged like everyone else's.
- A device reloads whenever the replay changes an audience's reset or
  schema version, from the snapshot the kept entry names, so a reset
  dropped by a later entry is followed by the winner's.
- A reset replaces that audience with the chosen snapshot. On every
  device, including its author, only these contents count:
  - the snapshot's rows and merge records;
  - later writes whose `store_log_read` includes the kept reset entry.
- A write outside the snapshot that had not read that entry is ignored
  for the reset audience, even if it read every write in the snapshot.
  - Do not apply its row changes or record them as lost.
  - Count the ignored part as passed, so log positions can advance.
  - Other audiences' parts follow their own reset and schema rules.
  - Reading an ignored pre-reset write does not invalidate a post-reset
    write: the reset deliberately discards that old history.
- A device still uploads its unsent pre-reset writes, keeping their
  identities and attempted bytes, so its log has no gaps. It ignores their
  effects in the reset audience itself, like every other device.
- E.g. Ana resets from her phone's snapshot, covering Ben's writes through 8.
  Ben's laptop made writes 9 and 10 without reading the reset; 10 read 9.
  - Ben uploads both. Neither changes the reset audience or creates a loss.
  - After reading the reset, Ben makes write 11. It applies on every device.
  - Ana's own writes made after capturing the snapshot but before reading
    her reset entry are ignored by the same rule.
- If two devices reset the same audience at once, the one with the smaller
  timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

## 20. Crates and conventions

### 20.1 Crates

- Coven is a Cargo workspace of eight crates, each owning one part of
  this spec:

| Crate | Owns | Spec |
| --- | --- | --- |
| `coven-foundation` | The clock, the id source, atomic file writes, the store's directory and its lock | [§7.2](#72-timestamps), [§10](#10-device-identity), [E1](api.md#e1-opening) |
| `coven-crypto` | Ciphers, sealed boxes, derived keys, file keys, member keys and their custody | [§11.1](#111-cryptography) |
| `coven-merge` | Timestamps, the merged state, the removal rules and lost values, as functions with no I/O | [§7](#7-order), [§8](#8-merge), [§14](#14-audiences) |
| `coven-format` | The bytes in storage: write records, store log entries, snapshots, file headers and chunks, encoded, decoded and checked, using merge's and crypto's types | [Appendix D](format.md) |
| `coven-database` | The SQLite connection, coven's internal tables, applying the merge's results, triggers, live queries, migrations | [§5](#5-local-database) |
| `coven-storage` | Each provider, and the operations coven needs from it, including upload sessions | [§4](#4-storage-providers-and-access) |
| `coven-sync` | Device logs, the store log, members, circles, snapshots, files and the cache, operations, recovery | [§6](#6-syncing-writes), [§9](#9-members-and-roles), [§12](#12-joining-and-restore) to [§19](#19-recovery) |
| `coven` | The API, and nothing else | [Appendix E](api.md#appendix-e-api) |

- Each crate depends only on crates above it in the table, except that
  `coven-database` and `coven-storage` never depend on each other.
- So the database never reaches storage, and storage never reads the
  database; `coven-sync` is where the two meet.
- `coven-merge` and `coven-format` read no clock, file, database or
  network.
- So the merge is tested, and checked against the Lean model of
  [Appendix B](proofs/merge.md), without SQLite or storage.
- Whole-history construction belongs to test support; production merges
  arriving writes against the applied metadata without retaining write bodies.
- Ids shared by several crates (store, member, device and circle ids) live in
  `coven-foundation`; each concept has one type.
- Each external dependency's version is set once, in the workspace, and
  crates name only the features they need.

### 20.2 Capabilities, owners and lifetimes

#### Capabilities

- A *capability* is something outside the program's own memory: the
  network, cryptography, SQLite, the OS keychain, the current time, new
  ids, and files on disk.
- Each capability is used directly in one place only:

  ```
  network          coven-storage's providers
  cryptography     coven-crypto
  SQLite           coven-database
  OS keychain      coven-crypto's custody
  current time     coven-foundation's clock
  new ids          coven-foundation's id source
  files on disk    coven-foundation's file writes, and the file cache
                   in coven-sync
  ```

- Everything else reaches a capability through the object that owns it,
  given to it when it is built.
  - The clock, the id source, key custody and the
    OAuth clients and presenter are all set on the builder ([E1](api.md#e1-opening)), so
    a test replaces each one.
  - E.g. the snapshot writer never calls the system clock; it asks the
    clock it was given, and a test gives it a fixed one.
- A method never takes a raw capability as a parameter, such as a store
  directory or a database connection; it calls the owner of it instead.

#### Owners and tasks

- An *owner* is an object that holds a capability, or holds another
  owner, and lives while the store is open.
- E.g. the database owner holds the SQLite connection, and the sync owner
  holds the database owner and the storage owner.
- A *task* is a value that lives for one piece of work and is then
  dropped, such as one sync pass, or one step of an operation
  ([§18](#18-operations)).
- A task borrows the owners it needs for that work, and holds nothing
  past it.
- Owners receive their collaborators when they are built.
- E.g. the sync owner takes the database owner and the storage owner as
  arguments, and doesn't open either itself.
- One authoritative list in the checker's policy names every place allowed
  to construct owners or start long-lived work. Each entry names an exact
  source file, type and method (or free function), not a whole file or a
  type-name suffix. These *composition roots* include opening and bootstrap,
  provider connection construction, owner constructors and task starts.
- The checker infers owners from retained capabilities and other owners.
  Every owner construction outside a listed root fails the check, including
  construction of an owner's private representation. Runtime acquisition
  and thread or task spawning use the same list. A legitimate new place
  must be added deliberately; returning an owner grants no construction
  permission. Test-only sources and items may assemble their own graphs.
- Each long-lived task has one owner responsible for starting and stopping
  it, including when that owner is dropped. Its start sites belong to the
  same list; there is no separate list of lifetime authorities.
- E.g. only the sync owner starts the sync loop, so closing the store stops
  it, and nothing else can leave one running.
- Closing a handle, or dropping its last clone, marks its store as closing
  before releasing its owners. Blocking work retains the database connections
  and writer lock until it finishes; subscriptions retain only their active
  database reads. Opening or bootstrapping that store in the
  same process waits for release, using a process-wide registry keyed by its
  canonical directory and store id and a notification when the lock handles
  close. There are no timeouts or polling sleeps. A live instance in this
  process, or a lock held by another process, still returns `AlreadyOpen`
  immediately ([E1](api.md#e1-opening)).
- Opening leaves configured storage `Stopped`, or `Disconnected` when none is
  set up. `start_sync` asks the injected provider connector to build a client
  when absent, using custody credentials, then starts the loop. Tokens refresh
  on rejection (§4). Repeated starts keep the running loop and client.
- `stop_sync` finishes the active pass and file transfers before releasing
  every worker's provider reference. Session keys stay with the open handle;
  closing or explicit key forgetting erases them. It retains the location
  and custody credentials for the next start. `disconnect_storage` also forgets
  this device's storage credentials, leaving remote contents untouched (E5).
- An owner never hands out what it holds, by returning it or by a public
  field; callers ask it to do the work.
- E.g. nothing outside coven-database gets the SQLite connection; it asks
  the database owner to run a write.
- A closed custody owner is represented by the absence of its held custody,
  without a separate closed flag. Storage calls serialize with closing;
  calls already started finish even when their app future is dropped. Sign-in
  remains cancellable while presenting or exchanging credentials (E5).
- A struct built only to be taken apart again, with every field public
  and no methods, is not used to pass collaborators; they are passed by
  name.

### 20.3 Code conventions

- Visibility:
  - nothing is `pub` that can't be reached from outside its crate;
  - `pub(in path)` and `super::super::` are not used: an item needed
    elsewhere moves to where both callers can see it;
  - `coven` re-exports the API at its root and keeps every module
    private.
- Errors are typed enums per crate; an error is never turned into text
  to be passed on, and nothing returns `Result<_, String>`.
- Failed app calls and whole-pass sync failures retain typed causes.
  `Offline` means the last attempt could not reach storage; `Failed` means
  storage answered with an error preventing the whole pass (E5).
  Every object or operation that cannot advance has a typed pending record,
  including maintenance, prerequisites and relevant signed peer reports
  (§19.1, E5). A persisted reason never substitutes text for its category.
- A source file holds at most 1,000 lines, and its tests live beside it
  in `<name>_tests.rs`.
- Crates offer shared fakes through `test-utils` where needed, such as an
  in-memory provider and a fixed clock. Test-only implementations stay behind
  injected dependencies; tests build the same object graph production does.

### 20.4 Checks

- One script runs every check. CI runs its code checks on every platform,
  and the Lean proofs once, on Linux, since a proof checks the same on any
  platform:
  - formatting, and clippy with warnings denied;
  - the dependency rules of [§20.1](#201-crates);
  - the rules of [§20.2](#202-capabilities-owners-and-lifetimes), by a
    checker that reads the syntax tree of every crate, including inside
    macro calls, against the policy file;
  - the checker's own guard tests: every file, owner and composition
    root the policy file names must exist, so the policy can't go stale;
  - the visibility and file-size rules of
    [§20.3](#203-code-conventions);
  - `cargo doc` with broken links denied;
  - every crate built without test code, so an item only tests use shows
    up as dead;
  - the tests, with all features and with none;
    - the guarantees in §3 are exercised through app calls where visible:
      committed writes survive dropping the handle and process exits at each
      post-commit boundary, remain queued and upload after reopening; app reads
      and writes finish while a sync pass is held;
    - removal's next write and snapshots open with the rotated keys and fail
      with the removed member's keys; identical file uploads store different
      ciphertext; snapshots delete covered logs and superseded own snapshots;
    - injected failures at each remote-write mutation roll back app rows,
      merge records, losses, fingerprints, positions and query notifications;
      these tests use fixed inputs, an injected clock and explicit barriers;
  - the Lean proofs, built from scratch, with no `sorry` and no axiom
    beyond Lean's own, and their differential tests against Rust.
- The checker is the first thing built, before any crate, so every rule
  holds from the first line of code.
- Tests control clocks, transfer limits and request completion. Upload boundary
  tests use byte-sized payloads; races wait for observed events, and interrupted
  operations are reopened until they report completion.
- The pre-commit hook runs the fast ones: formatting, clippy, and the
  rules of [§20.1](#201-crates) to [§20.3](#203-code-conventions).

## Appendix A. SQLite behavior in synced tables

Each SQLite feature whose meaning changes when devices write offline and
merge later.

### A.1 Integer primary keys

- Problem: two offline devices can pick the same id for different rows,
  and coven would treat them as one row.
- Status: refused; synced tables use UUIDs or keys derived from the
  content ([§8.5](#85-keys-and-uniqueness)).

### A.2 Tables with no primary key

- Problem: coven can't tell which row a change belongs to, and SQLite's
  hidden rowid collides across devices like an integer key.
- Status: refused ([§8.5](#85-keys-and-uniqueness)).

### A.3 Unique constraints besides the primary key

- Problem: two offline devices can each insert a row with the same value.
- Status: the row whose write has the smaller timestamp keeps the value;
  the other row is taken out while it conflicts, and recorded in
  `_coven_lost` ([§8.5](#85-keys-and-uniqueness)).

### A.4 Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is taken out while its parent is gone, and recorded in
  `_coven_lost` ([§8.4](#84-foreign-keys)).

### A.5 CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is taken out until it passes, and
  recorded in `_coven_lost` ([§8.6](#86-check-constraints)).

### A.6 Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write ([§8.7](#87-triggers)).

### A.7 Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: additions sync with no change of version; any other change
  raises the store's version, and every device updates and reloads ([§17](#17-schema-changes)).

### A.8 SET DEFAULT foreign keys

- Problem: the default parent is a second parent that merges like any row
  and can itself be gone. Supporting it requires restrict-like removal
  while that parent is gone, tracking its current generation, and handling
  rows that cannot be deleted when their defaults fail a CHECK.
- Status: refused on foreign keys of synced tables and foreign keys from
  local tables into synced tables, for both delete and update actions
  ([§8.4](#84-foreign-keys)). SET DEFAULT appears in so few SQL files that
  refusing it affects almost nobody. Counts of SQL files on GitHub using
  each action, from GitHub's REST code search (`GET /search/code`, query
  `"ON DELETE <action>" language:SQL`, its `total_count`), retrieved
  2026-10-07; approximate, across all SQL dialects:

  | Action | SQL files |
  | --- | --- |
  | `ON DELETE CASCADE` | ~1,714,000 |
  | `ON DELETE SET NULL` | ~646,000 |
  | `ON DELETE RESTRICT` | ~496,000 |
  | `ON DELETE NO ACTION` | ~115,000 |
  | `ON DELETE SET DEFAULT` | ~2,200 |

### A.9 References to non-key columns

- Problem: a parent can change a UNIQUE non-key value while another device
  inserts a child referring to the old value. Parent row identity and
  generation cannot track that mutable value; arrival order can either
  refuse COMMIT or let SQLite rewrite a synced child with ON UPDATE CASCADE.
- Status: refused for foreign keys of synced tables on open and after
  migrating; the target must be the parent's complete primary key
  ([§8.4](#84-foreign-keys)). Local children keep SQLite's behavior subject
  to the existing foreign-key action restrictions.
