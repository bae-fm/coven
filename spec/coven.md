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
  - [A.10 Collations on keys](#a10-collations-on-keys)
- [Appendix B. Proof of convergence](proofs/merge.md), in its own file
- [Appendix D. Storage format](format.md), in its own file
- [Appendix E. API](api.md), in its own file

## 1. Overview

- Sync for a person's own devices or a group of 1–20 people, median 4–5.
- The app's data is SQLite. The app keeps its schema and writes ordinary SQL.
- Rows can carry files, like audio, images or documents, which sync with
  them.
- No server. It syncs through storage the members already have.
- Contents are encrypted ([§11](#11-keys)); slot kinds, key-copy routing,
  object framing and S3 detection's probes are visible (D9).
- A slot's device, claim number and each part's audience are sealed under the
  store key: every member can read them, but the provider cannot.
- Only members' devices hold the store key.

## 2. Threat model

- The storage provider is assumed *honest but curious*, not *hostile*:
  - it may read what it stores;
  - it doesn't alter or withhold what it stores;
  - it sees object sizes, counts, timing, and which device writes.
- A thief with storage credentials but not the store key may be hostile.
- Deletion and withholding can't be prevented. No design can.
  - Coven does not recover lost or damaged storage. The household starts a
    new store by hand. The reset in §19.3 replaces an audience
    from a chosen device; it cannot rebuild storage.
- Current members trust each other: each has access to the storage and
  isn't hostile on it.
- An ex-member may keep a copy of the store key they had.
- Each person keeps their restore code to themselves, on paper or shown on
  a screen ([§12](#12-joining-and-restore)); anyone who sees it can open
  the store with storage access while its keys are current. Anyone holding
  that current pair is that member to coven; coven cannot tell them apart.
  Replacement cannot revoke access to historical keys and ciphertext already
  available to that pair; later old-credential writes follow §10's slot cut-off. Theft of a restore
  code is out of scope: coven offers no recovery from a stolen code. The
  household removes the member and starts over by hand if needed.

The guarantees assume correct ciphers, signatures, hashes, key derivation,
random generation and secret custody ([§11.1](#111-cryptography)). No Lean
model proves their security: authentication and sealing are supplied inputs.
Atomic, durable SQLite transactions and durable file/custody writes are also
assumed. The provider behavior above and in §4, §10 and §13—including complete
reads, atomic create-once publication, conditional replacement and confirmed
access cut-off—is assumed, not established for real providers by the models.

## 3. Guarantees

Proof links establish their named model results under stated assumptions.
They do not prove the Rust implementation. [§2](#2-threat-model) states the
cryptographic and environmental assumptions.

- **Local reads and writes:** app commits need no network and never wait for sync.
  Uncached file reads need storage (§16.3); admin operations need online catch-up (§9).
- A reset, breaking raise or skipped history can require an audience reload.
  That audience refuses writes with `AudienceReloading` until reload commits (§15).
- Other audiences remain writable. Ordinary snapshot reloads permit app writes throughout.
  The [queue model's `reloading_blocks_commit`](proofs/queue.md#availability-and-commit-refusals) proves the reload gate.
- Membership, audience and row-key checks use locally applied state (§8.5, §14).
  They can refuse an app write, but never request a network check at commit.
- Schema and migration checks can refuse opening or migration (§17).
  SQL constraints, triggers and coven's SQL access rules can refuse local writes (§5, §8).
- A migrated app can commit writes before its raise claims a slot.
  Those writes wait in its queue (§17.1).
- Self-replacement temporarily refuses app writes with `MemberKeysChanging` (§10).
  Timestamp, representation and file checks can also refuse writes (§6, §7.2, [E3](api.md#e3-writing)).
- Database, disk and rollback failures retain their causes.
  A closed or unusable installation cannot accept writes ([E1](api.md#e1-opening)).
- File cleanup failure after commit leaves the committed result returned (§16.6).
  No proof establishes that the Rust implementation avoids every network wait.
- **Confidentiality:** the provider and outsiders cannot read encrypted contents.
  Circle data is readable only by its members (§11).
- Slot kinds and key-copy routing are clear. Names, values, key commitments
  and operation contents stay encrypted. The store key also seals each slot's
  device, claim number and part audiences; circle keys seal circle-part contents.
  Keyed authorship roots prevent guessing values without the secret (D11).
- **Authorization:** signatures identify authors. Admin actions use the preceding
  slot's state; writes use their authored view and the removal cut-off (§9, §10).
- Only authorized members change membership, and at least one admin remains.
  Lean assumes cryptographic security; it does not prove it.
- **Atomicity:** a slot's effects and applied number commit together (§6).
  Its verdicts and losses cannot become visible with partial progress. SQLite atomicity is assumed.
- **Convergence:** one applied prefix determines membership and each readable
  audience's data at a given schema
  ([`prefix_determines_state`](proofs/sequence/CovenSequence/Converge.lean),
  with deterministic admin, merge and conversion rules supplied to the model).
  Timestamps select cell values; generations defeat edits concurrent with deletion.
- First slot selects unique-value winners and original row owners (§8.5, §8.8).
  Original-value and dependency losses remain available. Removal refusals and reset-ignored parts create no losses (§8, §19.3).
- **Durability:** commits and operation journals survive crashes while the database
  and custody remain usable. Resetting an unusable installation reports discarded unsent edits (§10).
- Queued writes eventually claim with successful prerequisites, storage and fair
  claim progress. Contention has no fairness bound.
- Admin work either claims or undoes its recorded artifacts.
  Failed undo remains journaled and reported (§18).
- **Revocation:** catch up before sending. First attempts use the newest usable key
  (§10, §11; [`revocation`](proofs/sequence/CovenSequence/Keys.lean)).
  A blocked slot follows [§6](#stuck-devices), so a later removal cannot be read past it.
- Writes claimed after their device or member is removed, or their signing
  credential retires, have no effects.
  Record their refusal; there is no grace period for late claims (§10).
- Prior attempts retain their inner bytes and keys. Provider cut-off remains separate;
  in-flight key copies retain §11's residual disclosure window.
- Leaving a circle still permits previously authored circle edits (§14.6).
- **Storage bounds:** snapshot coverage and grace permit deletion without waiting
  for absent devices. Returning devices reload and retain eligible unsent writes (§15).
- From readiness, a prefix is deleted within U+G+P+D, assuming successful
  snapshot preparation, upload and announcement within U, grace G, a successful
  conditional floor update within P after grace, and deletion within D after that
  update ([`bounded_retention`](proofs/sequence/CovenSequence/Deadline.lean)).
- Every relevant audience must supply covering snapshots that remain usable
  through the floor update. Missing coverage prevents the retention gate;
  an audience without a reader to supply it has no deletion deadline
  ([`uncovered_has_no_deadline`](proofs/sequence/CovenSequence/Deadline.lean)).
- The bound does not limit live rows, losses, authentication evidence or an unbounded write rate.
- **Visible failures:** a blocked slot, operation, file or cleanup names its
  first unmet condition to the local app (§19.1). Eligible local work continues.

The [sequence proof](proofs/sequence.md) states each result's assumptions and
model boundaries. Merge and conversion proofs cover their stated inputs;
they do not establish the composed storage protocol.

### 3.1 IO bounds

[One sync pass](sync-pass.md#counting-a-pass) counts the requests for each kind of work.

- An idle pass makes 2 requests: read the next slot, which is absent, and read
  the floor. It lists nothing and writes nothing.
- Count provider metadata, redirects, pages, upload parts and retries too.
  Report these extra requests separately from the two reads above.
- A complete object needs at most two prefix reads and one remaining-body
  stream. Charge native requests separately when a provider splits that stream.
- Finding the end from far behind takes logarithmic existence checks.
  Applying the missing history still reads each needed slot, or reloads from snapshots.
- Contention adds creates, occupied-slot reads and retries. There is no bound
  on how many times a device can lose a claim.
- More unchanged history or files must not increase an idle pass's work.
  An idle pass does no full-history decoding and commits no database transaction.
- Unlock custody once per open handle. Sync passes do not repeat passphrase derivation.
- Never hold the database writer across network requests, custody calls,
  decoding, timers or file IO.
- Automatic retries follow the persisted delays in [§16.5](#165-uploads-and-deletion).
  Completing a pass does not reset a delay.
- Tests count requests, bytes, custody derivations, decoding and durable commits.
  They cover reopening, contention, interrupted reads, blocked slots and floor changes.

Ana's idle store needs the same two reads with three devices or twenty.
Receiving Ben's note adds its slot read.

## 4. Storage providers and access

- A store lives on one provider: S3, Dropbox, OneDrive, or iCloud through
  CloudKit.
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
    Replacing the floor retains its fetched change tag and uses
    `ifServerRecordUnchanged`.
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
    so a scoped object listing uses one equality query. Sequence
    discovery uses exact record ids, not queries. Object metadata queries
    request only `path` and `size`, exclude assets and follow every
    [CKQueryOperation cursor](https://developer.apple.com/documentation/cloudkit/ckqueryoperation).
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
- For Dropbox and OneDrive, the app triggers `authenticate` on
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
    (`If-None-Match: *` or `x-goog-if-generation-match: 0`); copy
    publication (`S3CopyPublication`): never, so single PUTs and multipart
    completions carry the create-once condition; multipart uploads only; or
    every object, single-request creates included, each by copy with a
    destination condition (`If-None-Match: *`, `x-goog-if-generation-match: 0`
    or `cf-copy-destination-if-none-match: *`); the native multipart timestamp
    (completion or start); the checksum policy (scope stated, unstated scope
    ignored, or none returned); listing V2 or V1; ETag or generation revision;
    S3 or Google response headers; and
    whether reads request checksums. Each behavior is implemented once in the
    adapter; apps do not supply capability flags or service branches.
  - A location's profile is one of three: none, selecting by endpoint host; a
    known service's name; or a custom profile carrying its own complete
    `S3Settings`. The location records it as given, so restore codes and every
    other device use identical behavior.
  - Known services are const entries in the adapter's
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
    overrides a known host: such an endpoint admits only its own
    profile's name. Without an endpoint, requests reach AWS, so only AWS may
    be named and no custom profile is accepted. Known hosts require
    `https`; other endpoints may use plain `http`, as a self-hosted server on
    the same machine or a private network can. A contradicting name or custom
    profile, or a plain-`http` known host, fails validation with
    `InvalidConfiguration` before any request. The self-hosted profiles are
    **RustFS**, **SeaweedFS**, **Ceph RGW** and **Versity Gateway**; no
    hostname selects them.
  - **Member key scope.** A service limits a member key either to a prefix
    inside a bucket or only to a whole bucket. AWS (IAM `s3:prefix`
    conditions), GCS (a managed folder per store), RustFS, SeaweedFS, Ceph RGW
    and Versity Gateway (policy `s3:prefix` conditions) limit it to the
    store's prefix, so their
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
      Any object or folder other than the store's own `<store>/` and the
      fixed root object `coven-bucket` refuses setup with
      `OwnBucketRequired { reason: Occupied }`. The listing checks for
      preexisting objects older than the listing margin M; objects stored
      within M may be absent.
    - After detection and the provider check pass, and before publishing
      store state or recording its location, first setup creates
      `coven-bucket` once, with the same primitive slots use. Its complete
      bytes are this store id's canonical lowercase hyphenated UUID, 36 ASCII
      bytes without a newline (D10). A successful create proceeds. An
      occupied name is read exactly: equal bytes mean a retried setup and
      proceed; any other bytes refuse with `OwnBucketRequired { reason:
      Occupied }`. At most one store's setup passes per bucket, independent
      of listing lag ([`at_most_one_store`](proofs/storage/CovenStorage/BucketClaim.lean)).
      The marker is never replaced or deleted. Failed setup retains it for
      the same store's retry. Deleting a store from a device (`delete_store`
      or `discard_store`, E1) leaves this remote marker, like its other
      stored objects, untouched.
    - Reconnecting, restore and joining use the recorded location and check
      only their existing checks plus an exact read of `coven-bucket`.
      They require its bytes to name this store; a missing, malformed or
      other-store marker refuses with `OwnBucketRequired { reason: Occupied }`.
      They neither list the root nor create a missing marker. Read failures
      retain their provider causes.
    - Moving a store into its own bucket at its first invite would copy all
      its data, so the rule applies from creation, including to one-member
      stores.
    - E.g. Ana creates Household in the R2 bucket `household`. Creating
      Garden in that bucket is refused; Garden gets the bucket `garden`.
  - An endpoint that matches no known host and names no profile has no
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
    detected `S3Settings` and one typed result per probe, each saying
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
    result. Pauses use the store's clock capability. No other probe
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
    known profile's. Coven addresses current objects; it does not choose
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
    owner setting. This exception requires R2's known hostname, not an
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
    - `Behavior`: another setting contradicts a result: a signer the
      service refused, a listing protocol that failed or disagreed, no
      checksum policy where one was returned, stated scope where scope was
      missing, checksum requests the service refused, generation revision or
      Google headers without GCS's headers, S3 headers where GCS's arrived,
      or a refused zero-entry listing at a missing object's key.
    A manual override contradicting the results is refused this way and
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
    completions both succeeded, so SeaweedFS's suite 2 certificate was
    withdrawn. Suite version 5, which races the named entry's own
    publication, passed SeaweedFS's multipart copy publication with
    versioning disabled and enabled on **2026-10-14**, but with versioning
    disabled a conditional PUT racing a conditional copy of one name both
    succeeded in 14 of 200 trials. Its entry now publishes every object by
    copy, and on **2026-10-15** suite 5 passed it with versioning disabled
    and enabled. A rerun later that day failed its multipart and mixed races
    three times, but it had named the profile in the env file, which the
    suite then ignored, so it raced the detected settings' conditional
    completion, not the entry's copies. Run again as the named entry, suite 5
    passed it three times with versioning disabled and three times with it
    enabled; its certificates cover both states. On 2026-10-14 suite 5 passed R2, RustFS
    1.0.1 with versioning disabled and enabled, and Ceph RGW 20.2.4 and GCS
    with versioning disabled; those entries record suite 5 certificates. It
    also passed Versity Gateway 1.9.0 with versioning disabled and enabled
    from detected settings, and on **2026-10-15** again as the named
    `VersityGw` entry, whose certificates cover both states. AWS
    is **uncertified**: there is no live account. The table's selected
    behaviors are:

    | Profile | Signing | Single PUT | Copy publication | Native multipart time | Checksums | Listing | Revision |
    | --- | --- | --- | --- | --- | --- | --- | --- |
    | AWS (uncertified) | AWS SigV4 | `If-None-Match: *` | Multipart, `If-None-Match: *` | Start | Scope stated | V2 | ETag |
    | GCS (certified) | Google HMAC | `x-goog-if-generation-match: 0` | Multipart, the same generation condition | Completion | Scope stated | V2 | Generation |
    | R2 (certified) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | Unstated scope ignored | V2 | ETag |
    | RustFS (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | None returned | V2 | ETag |
    | SeaweedFS (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Every object, `If-None-Match: *` | Completion | None returned | V2 | ETag |
    | Ceph RGW (certified, unversioned) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | None returned | V2 | ETag |
    | Versity Gateway (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | Scope stated | V2 | ETag |

    RustFS, SeaweedFS and Versity Gateway support versioned buckets: RustFS
    1.0.1, SeaweedFS 4.48 publishing every object by copy, and Versity Gateway
    1.9.0 (POSIX backend with `--versioning-dir`) passed every suite 5 race
    with versioning enabled. GCS ran only with versioning disabled, so it
    has no versioned-bucket evidence. AWS remains uncertified. R2 has no versioning feature. Ceph
    RGW 20.2.4 fails the versioned races. See the
    [suite 4 evidence](s3-certification.md#concurrent-certification-suite-4),
    the [full battery](s3-certification.md#full-battery-2026-10-14), the
    [GCS and Versity Gateway runs](s3-certification.md#gcs-and-versity-gateway-2026-10-14),
    the [SeaweedFS every-object run](s3-certification.md#seaweedfs-every-object-copy-publication-2026-10-15),
    the [SeaweedFS rerun](s3-certification.md#seaweedfs-rerun-2026-10-15)
    and the [named-entry reruns](s3-certification.md#seaweedfs-reruns-as-the-named-entry-2026-10-15)
    for the live runs and their bucket states.

    AWS, R2 and Versity Gateway request checksums; every profile but GCS consumes S3
    response headers. RustFS, SeaweedFS and Ceph RGW are not asked for checksums they
    never return. GCS supplies
    its hashes without a checksum-mode request; the adapter translates Google
    headers. The ETag revision behavior uses native versions or `coven-revision`
    metadata for replaceable objects, as specified below.
  - To certify a service, put its bucket, region, endpoint and HMAC keys in an
    env file using `COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`.
    Run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification -- --ignored --nocapture`.
    Select a self-hosted profile with `COVEN_LIVE_S3_PROFILE=CephRgw`
    (or `RustFs` / `SeaweedFs` / `VersityGw`) in the process environment;
    otherwise the suite detects its settings. The env file holds only the
    five settings above: the suite refuses a file holding any other, so a
    profile name written there cannot be silently ignored.
    The ignored test runs production detection from the location's settings,
    or the plain S3 settings where detection must supply them, and prints
    every result. A detected entry that fails the setup safety
    check above prints as uncertified with its first failed check. From suite
    version 5 a named profile's own entry is the candidate, checked against
    those results; otherwise the detected settings are. Sequential probes
    cannot show that a race needs copy publication, so detection alone would
    race SeaweedFS's conditional completion instead of its copy, and
    detection never chooses copy publication for every object. A profile
    publishing every object by copy races its single-request creates as
    copies too, so its PUT and mixed races are copies against copies. A profile
    setup refuses prints that verdict separately. The candidate
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
    required, not results exempted from failure. A service that
    returns no checksum on any probe, even with checksum mode, is recorded as
    returning none: §4 permits a missing checksum, so suite version 2 accepts
    that result where version 1 failed detection. Each §4 requirement
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
    exact prefix and every continuation token.
  - GCS ignores `If-None-Match: *` on PUT. Create-once PUT instead sends
    `x-goog-if-generation-match: 0`. An occupied destination returns HTTP 412
    `PreconditionFailed`, mapped to `AlreadyExists`, without changing its
    bytes or publication time. Conditional replacements use the observed
    generation in `x-goog-if-generation-match`; each replacement's revision
    is its new `x-goog-generation`, including equal-byte replacements.
    Requests use Google V4 HMAC authentication and `x-goog-*` headers: GCS
    refuses requests mixing Google and AWS extension headers.
  - GCS XML multipart initiation rejects generation conditions and completion
    does not enforce create-once, even though its measured native timestamp is
    completion. Coven therefore publishes by copy. Every copy-publication
    profile uploads unconditionally to
    `<prefix>/<store>/.coven-uploads/<session>`, where `<session>` is the
    upload's coven-generated UUID; other profiles upload at the destination,
    the shared slot path or an immutable artifact path. Every S3 session is recorded with its
    name before its native multipart upload exists: the first part initiates
    it, and the recording then gains the native id. A recording that
    predates an initiation whose reply was lost retains its unresolved
    session. Recovery may abort only uploads proven to belong to that
    session; a shared sequence destination does not establish ownership.
    If the native id cannot be recovered, report `SessionIdentityUnknown`
    and retain the journal until provider evidence settles cleanup. A
    private staging name can identify its own uploads. Aborting never
    deletes a destination or another claimant’s session. The reserved namespace is distinct from every
    coven object path in D10. Retrying or resuming a recording derives the
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
  - A profile publishing every object by copy (SeaweedFS) publishes a
    single-request create the same way: one unconditional PUT stages its
    bytes at `<prefix>/<store>/.coven-uploads/<create>`, carrying a fresh
    attempt id as its marker, then one conditional copy publishes it.
    `<create>` is the create's own name: the UUID formed from the first 16
    bytes of SHA-256 over `coven/copy-temporary/v1`, the destination path's
    length as a big-endian u64, the path and the bytes.
    Sequence slots below the single-request limit are such creates, each still dated by one request's
    completion: the copy's. A refused copy, or one whose reply was lost,
    settles as stored when the destination carries its marker; otherwise
    the copy's answer stands. The temporary is deleted whatever the copy's
    outcome, and a failed deletion fails the call with both errors
    preserved.
  - Concurrent attempts of one create, such as two creators of one path
    with identical bytes, share its temporary. One can delete it between
    another's staging and copy, so that copy finds no source. Its answer
    says nothing about the destination, which settles it instead: its
    marker is stored, another object is an occupied path whose bytes §10
    compares (the create's own when only its attempts publish there), and
    an empty destination is a retryable `Conflict` the caller retries under
    its kept record. No attempt ends in a hard error or loses the create
    ([`shared_never_fails`, `shared_occupied_identical`, `attemptCopy_retry_empty`, `retry_publishes`, `shared_temporary_missing`](proofs/storage/CovenStorage/CopyPublication.lean)).
    The marker names the attempt that staged the bytes the copy carried,
    not the copy: an attempt staging over another's before that one copies
    sees its own marker at the destination, so both answer stored, which
    equal bytes settle to anyway
    ([`shared_temporary_both_stored`](proofs/storage/CovenStorage/CopyPublication.lean)).
  - Every temporary is staged under a name its publisher has already
    recorded, and stays recorded until it is deleted. Every attempt of one
    create stages and deletes the same name, so the caller's record of the
    create, which it keeps with its fixed bytes until the create settles
    (§6, §10), records the temporary too. A retry after a crash between
    staging and deletion, or after a failed deletion, removes it, and finds
    its own bytes at the destination as an occupied path, which §10's byte
    comparison accepts as the same create. Under a fresh name per attempt a
    crash would strand a temporary no record names
    ([`staged_recorded`, `retry_clears`, `failed_deletion_retry_accepted`, `attempt_naming_strands`](proofs/storage/CovenStorage/CopyPublication.lean)).
  - Given a copy that checks and publishes its destination atomically,
    copy publication is create-once: the copy changes published objects
    exactly as a create does, a temporary is never visible as a published
    object, and of sessions racing on one name the first copy to arrive
    publishes its own bytes while every other copy refuses the condition and
    deletes its temporary. A lost copy reply settles by reading this session's
    marker at the destination; since a session's bytes are fixed, its marker
    means its bytes ([`copy_is_create`, `stage_hidden`, `race_one_winner`,
    `lost_reply_settles`, `marker_settles_bytes`](proofs/storage/CovenStorage/CopyPublication.lean)). Atomicity is
    the provider property certification's races test: a completion that checks
    a free name and then writes it lets two racers both succeed
    ([`check_then_write_two_winners`](proofs/storage/CovenStorage/CopyPublication.lean)).
    When every publication of a name is a conditional copy, at most one is
    accepted, in any arrival order, given only that the provider applies one
    name's copies one at a time; none is when the name was already taken
    ([`every_copy_at_most_one`, `copied_at_most`](proofs/storage/CovenStorage/CopyPublication.lean)).
    That assumption says nothing of a conditional PUT racing a copy: a PUT
    that checks a free name, then writes it after a copy landed, is accepted
    beside the copy
    ([`put_racing_copy_two_winners`](proofs/storage/CovenStorage/CopyPublication.lean)).
    A profile whose service keeps copies, but not PUTs, exclusive against
    copies therefore publishes every object by copy.
  - One S3 `CopyObject` copies at most 5 GiB. A profile publishing by S3
    copy (AWS, SeaweedFS) refuses a larger upload before any request with
    `StorageFailure::CopyTooLarge`; such an object cannot be published
    atomically there. A batch, snapshot or file beyond that provider limit
    waits with that named failure; the sequence format adds no batch cap.
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
  - Every S3 location, known, named, custom or detected, runs the setup
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
  - RustFS and unversioned Ceph RGW refused occupied names with HTTP 412 on
    PUT and multipart completion, preserving bytes and times, and their
    multipart `Last-Modified` fell between the completion markers, so they use
    conditional completion without a copy. That sequential refusal does not
    hold under concurrency on SeaweedFS 4.48: two racing conditional
    completions of one name both succeed, and in an unversioned bucket a
    conditional PUT racing a conditional copy of one name both succeed too,
    while copies racing copies keep one winner. The SeaweedFS profile
    therefore publishes every object by copy with `If-None-Match: *`. None returned a checksum, even
    with checksum mode, so their profiles do not request one.
    Versity Gateway 1.9.0, versioned or not, refused occupied names the same
    way, sequentially and in every suite 5 race, so it uses conditional
    completion too; unlike the others it returns checksums with their scope
    stated, so its profile requests them.
  - `RequestTimeTooSkewed` means the device clock is off, not that access
    was denied. Retry the request once, signed with the server offset its
    response's `Date` states.
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
  - atomically create an object once, including a shared sequence slot,
    refusing an occupied path without replacing its bytes,
    in one request or, past the provider's single request limit, through its
    resumable upload;
  - read it, whole and by range; complete-object reads stream with bounded
    buffers so the reader checks chunks without a request per chunk
    ([One sync pass](sync-pass.md#one-stream-checked-as-it-arrives));
  - list a prefix, with when storage stored each object; each query is scoped
    to that prefix, across all its pages, without a change feed;
  - get one object's status by its exact path: absent, or its complete
    encrypted size, first publication or replacement time, object id and
    revision. A revision changes when replacement changes the encrypted bytes,
    even when size and timestamp match. Identical-byte writes may retain a
    revision (Dropbox does); they do not change a reader's retained bytes.
    An error is never absence;
  - delete;
  - grant and revoke a member's access, where the provider can: Dropbox,
    OneDrive and iCloud share with an account.
- On Dropbox and OneDrive, every directory component in D10 is a
  real provider folder. List that folder directly, never the configured
  root followed by client-side filtering. S3 uses the exact key prefix;
  CloudKit queries the exact indexed ancestor directory in the selected zone.
  Folder ids stay with the connection.
  A failed listing cannot establish that all objects were found.

  - S3 and Dropbox downloads include their metadata without a preliminary
    status request. OneDrive fetches exact metadata before downloading. Its
    replaceable objects also require a metadata recheck after downloading
    to reject a concurrent replacement. Count metadata requests and redirects
    in `M`; a provider error is not absence.
  - Ana's `files/` folder can hold 20,000 photos. Reading `seq/41`
    never enumerates them or another store's objects.
  - A store's folder or prefix can also hold things coven never wrote: a
    person's files and folders, native documents, shortcuts, packages or
    another store's folder. [D10](format.md#d10-paths) gives each object
    one spelling and no reader looks anywhere else, so a listing skips
    every entry that is not a D10 path. What
    the provider itself gets wrong still fails the listing: an entry
    outside the requested scope, missing metadata, or a failed or
    incomplete page.
- Listings may lag publication. Their measured lag remains a provider
  setup measurement, not a sequence correctness rule. Sequence,
  snapshot and member discovery use exact paths and authenticated slots.
  Exact reads and status must reflect completed publication. A provider
  error is never absence. Destructive orphan scans require a complete
  post-cut-off listing, independent of ordinary listing lag (§16.5).
- Create-once is required on every supported provider, under simultaneous
  claims as well as retries. At most one distinct byte string may occupy a
  slot, including multipart publication. `AlreadyExists` can reach the
  winner after a lost reply; complete-byte comparison decides it (§6).
  A provider lacking this property cannot hold a sequence store.
- The floor requires atomic conditional replacement by revision. A failed
  condition changes nothing; its caller rereads and validates the new floor.
  Setup tests a matching replacement and refusal with a stale revision;
  unsupported conditional replacement refuses the provider connection.
- Status uses these calls and provider timestamp granularities. Retain the
  resolution `δ` from the returned timestamp's representation:
  - S3 HEAD: `Last-Modified`, in whole seconds, so `δ = 1 second`
    ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)).
    S3 listings use that same precision: truncate fractional XML listing times
    to whole seconds. GCS listings include milliseconds while HEAD and GET
    omit them; keeping both representations would give one immutable publication
    different times depending on the read.
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

  For OneDrive, `p` returned fractional-second digits give
  `δ = 10^(-p) seconds`: no fraction means one second, three digits one
  millisecond. Preserve the returned precision in comparisons; do not round
  a fractional timestamp to seconds or invent an order within an equal time.
  Keep provider times as integer seconds and nanoseconds: a platform clock
  type such as Windows's 100 ns `SystemTime` would drop returned digits.
  Preserve that precision independently of listing lag.

  Exact status and reads carry the complete encrypted size, native object id,
  revision, timestamp and its resolution, plus any returned checksum. A ranged
  read still describes the complete object. OneDrive uses `eTag`, and
  Dropbox uses `rev`. CloudKit uses the record's `recordChangeTag`. On replaceable S3 paths, GCS uses its generation; AWS and R2 use a
  version id when versioning is enabled, otherwise a fresh `coven-revision`
  value stored atomically in the replacement's user metadata. Immutable S3
  objects use their ETag. S3 listings fetch exact status for replaceable paths
  because `ListObjectsV2` contains neither versions nor user metadata.

  Checksums name their algorithm, encoding and scope: whole-object or
  multipart composite. An ETag or revision is never inferred to be a checksum.
  Dropbox exposes its block-based content hash, OneDrive SHA-1 or
  QuickXorHash, and S3 its explicitly returned checksum and scope. GCS's
  CRC32C is a whole-object checksum. R2's unscoped checksum is absent from
  coven's metadata; its scope is never guessed. RustFS, SeaweedFS and Ceph RGW return
  none; a checksum returned under their profile, scoped or not, contradicts
  the certified result and fails as a protocol error rather than being
  used. Metadata and downloaded bytes must describe the same revision:
  OneDrive rechecks replaceable metadata after download and fails if it
  changed.

  File status and upload confirmation use this call, not a folder listing.
  Count any missing-container check separately. S3 HEAD's
  bodyless 404 requires a bucket check: every S3 service uses a zero-entry
  listing with the exact object's key as its prefix, under the configured
  prefix and store id. Known profiles use ListObjectsV2; a custom V1
  profile uses ListObjects. Where the service allows it, a member key is
  scoped to the store's prefix; HEAD Bucket requires bucket-list permission
  without a prefix condition, which that key does not grant. Object absence
  comes from the exact HEAD, never from the listing. A miss remains two requests: one HEAD and one listing.
  A revision proves freshness, not byte
  equality: occupied immutable paths still require §10's complete comparison.
- Storage times come from the provider. Immutable objects retain their first
  complete publication time; replacing the floor records the
  replacement time. These times describe provider results, not slot
  order or a deadline for accepting writes or admin entries.
- Native publication gaps remain visible in the profiles and certification.
  GCS conditional-copy timestamps and R2 single-PUT and multipart timestamps
  passed the completion checks, as did RustFS, SeaweedFS, unversioned Ceph
  RGW and Versity Gateway single-PUT and multipart timestamps.
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
- Shared sequence paths have competing writers. Each claim attempt fixes its
  bytes, and different contents are ordinary contention (§6). Device-qualified
  artifact paths still have one intended writer and use §10's identity checks.
- On the providers that share with an account, only the member whose
  account holds the store can share it and take it back, so:
  - that member's devices make invites ([§12.2](#122-adding-a-person));
  - any admin removes a member, and that member's access is taken back by
    the owner's device when it applies the removal
    ([§13](#13-removing-members-and-devices));
  - coven promises the access intended by the applied sequence state. Provider
    requests take time, can fail, and need not match that intention at
    every instant;
  - each owner's device runs its grant and revoke requests one at a time.
    Before the next request it reads the current intended access. If a
    completed request has become obsolete, it records and performs the
    opposite request before reporting that account's work complete;
  - pending and failed requests remain in the existing operation journal
    and the pending list (§18, §19.1). Security work starts immediately;
    it follows the applied entry immediately.
  - Sharing is per account, not per invite: taking access back takes the
    account's access, whatever shared it.
  - So the owner's device leaves an account shared while a member in its
    applied membership reaches storage through it, or another of its own open
    invites names it.
  - An invite made at the same time on another of the owner's devices can
    still lose its access this way: the invited device's join fails with
    the provider's permission error, and an admin invites them again, as
    with access taken back in [§13](#13-removing-members-and-devices).
  - Taking access back removes only what grants that one account; a share
    that also grants others is left, and reported to the owner, who
    changes it in the provider.
- Every storage setup, including reconnecting an existing device, checks the
  candidate provider with the supplied credentials before preparing an admin
  claim or committing credentials, keys, location or restore code. It creates a
  fresh sealed test object, requires a second create to be refused, verifies
  whole and ranged reads, and requires its scoped listing query to succeed.
  If that query returns the fresh probe, its size must agree; absence inside
  `M` is permitted and establishes no completeness claim. The check then deletes
  the object and verifies absence by exact-id read. On S3 the safety check
  above runs first. A failure names the failed check and preserves the
  previous connection ([E5](api.md#e5-storage-and-sync)).
- Apart from the own-bucket marker `coven-bucket`, every storage path starts
  with the store's id, including the provider
  check's temporary objects and S3 detection's reserved
  `<store>/.coven-check/` probes ([D10](format.md#d10-paths)).
  - Several stores can share one folder or zone, and one bucket prefix
    where member keys can be limited to a prefix. On an S3 service whose
    keys reach a whole bucket, each store has its own bucket (above).
  - Setup writes only this store's prefix and reads only it, apart from
    the own-bucket marker and the bucket-root listing for first setup
    (above). At prefix-scoped locations, other stores and unrelated objects
    outside this prefix do not compete with this store. S3's read-only
    bucket-versioning query has bucket scope
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
  never enter admin entries, invites, logs or device key custody; member
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
    paste an admin access key or API token; a pasted key for Versity Gateway
    names its IAM endpoint too (below). A CLI runs only after the person
    chooses it, with no input; its output is parsed in memory and erased, and
    a failure reports only its exit status, since CLI text can echo
    configuration. Guided steps show the bucket, the store's prefix and the
    member policy below, filled in.
  - Each known profile's const entry names its key minter, function
    pointers that make, roll back, delete and read a member key with admin
    access, and list a store's keys. AWS uses IAM at its global endpoint,
    signing for `us-east-1`: a user named with the key name below, under the
    path `/coven/`, tagged `coven-store` and `coven-member` with the store id
    and the member or invite id (never an email), the member policy below as
    the inline policy `coven-member`, and one access key. SeaweedFS's
    embedded IAM, on the location's S3 endpoint and region
    (the operator sets `-iam.readOnly=false`), takes the same policy through
    `CreatePolicy` and `AttachUserPolicy`: its `PutUserPolicy` drops
    conditions and refuses `s3:ListMultipartUploadParts`. Ceph RGW accounts
    (Squid or newer) take the inline form at the location's RGW endpoint and
    region, using an account-root key or a user's delegated IAM access
    ([RGW IAM](https://docs.ceph.com/en/squid/radosgw/iam/)).
    Versity Gateway takes the inline form from its standalone IAM service
    (`versitygw iam`, which the gateway reads through
    `--iam-standalone-endpoint`). That service listens at its own address,
    apart from the S3 endpoint, so a pasted admin key for Versity Gateway
    names its IAM endpoint (`AdminCredential::AccessKey`'s `iam_endpoint`),
    signed for the location's region. Every other service's admin access
    names none, and admin access naming one for them, or none for Versity
    Gateway, fails with `InvalidConfiguration` before any request. The
    gateway's own admin API (`versitygw admin create-user`) has no per-user
    policy and is not used. GCS, R2 and RustFS have their own minters; a
    custom profile uses guided console steps. On **2026-10-14** the RustFS,
    SeaweedFS and Ceph RGW 20.2.4 minters, and on **2026-10-15** the Versity
    Gateway 1.9.0 and GCS minters, each made a key live with admin access, passed the member-side check below, were
    refused writes, reads and listings outside the store's prefix, revoked
    the key twice, read it absent and saw the service refuse it
    ([evidence](s3-certification.md#member-keys)). Run
    `COVEN_LIVE_S3_ENV=<file holding the admin key> COVEN_LIVE_S3_PROFILE=<name>
    cargo test -p coven-storage s3_member_key_lifecycle -- --ignored --nocapture`,
    adding `COVEN_LIVE_S3_IAM_ENDPOINT=<url>` for Versity Gateway; GCS's admin
    access is the signed-in `gcloud` account's.
  - On GCS each member key is the HMAC key of its own service account. The
    minter reads the bucket's project through the JSON API and refuses a
    bucket without uniform bucket-level access, which managed folders need.
    It creates the service account `<name>@<project>.iam.gserviceaccount.com`
    under the short name, whose description is the key name followed by
    ` member <member or invite id>`, creates the store's managed
    folder `<prefix>/<store>/` if absent, and binds the account to it as
    `roles/storage.objectUser` and to the bucket as
    `roles/storage.bucketViewer`, whose only bucket permission is
    `storage.buckets.get`, retrying once when a policy changes concurrently. Then `hmacKeys?serviceAccountEmail=` creates the key, whose
    access id is the member key id. The admin access is an OAuth access token
    with the `cloud-platform` scope. Google can take 60 seconds to see a new
    account or key; the retry window is 60 seconds.
  - Revoking a GCS key deletes it and, while IAM has the account, its other
    HMAC keys, then both bindings and the account; the store's managed
    folder stays. Absence is read from the key: `DELETED` or gone.
  - On R2 each member key is a Cloudflare account API token, named with the
    key name followed by ` member <member or invite id>`, and limited to the
    store's own bucket
    (`com.cloudflare.edge.r2.bucket.<account>_<jurisdiction>_<bucket>`) with
    the `Workers R2 Storage Bucket Item Write` permission group, object reads
    and writes. The location's endpoint host names the account and its
    jurisdiction. The token id is the S3 key id and the hex SHA-256 of the
    token value its secret. The admin access is an API token allowed to write account tokens,
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
    deletes it. Each account takes the short name, and its description is the
    key name followed by ` member <member or invite id>`.
    Requests are signed with Signature Version 4 for `s3`. Revocation calls
    `delete-service-account`; absence is read with `info-service-account`.
    RustFS answers a delete of an absent account with HTTP 400
    `InvalidRequest`, a status other refusals share, so a refused delete
    succeeds only when `info-service-account` then reads the account absent.
  - The member policy allows `s3:GetObject`, `s3:PutObject`,
    `s3:DeleteObject`, `s3:AbortMultipartUpload` and
    `s3:ListMultipartUploadParts` on `arn:aws:s3:::<bucket>/<prefix>/<store>/*`,
    and `s3:ListBucket` on the bucket under the condition `StringLike`
    `s3:prefix` `<prefix>/<store>/*`. An unconditioned statement allows
    `s3:GetBucketVersioning` and `s3:ListBucketMultipartUploads`, which AWS
    cannot limit to a prefix; neither reads an object. It never adds a `Deny` on
    `StringNotLike` `s3:prefix`: a request carrying no prefix, such as a bucket
    check, matches the negated condition and would be denied.
  - Making a key is one journaled operation (§18). Each mint call names its
    principal from a fresh random id, deletes nothing, and makes and checks
    the key (below) before its secret joins the journal, so a second live
    copy of the database never takes a key this one made
    ([`run_keeps_provider`](proofs/storage/CovenStorage/KeyDelivery.lean),
    [`clone_mint_spares_recorded_key`](proofs/storage/CovenStorage/KeyListing.lean)).
    The short name is `coven-` and 24 hex digits of the call's id, for
    GCS service accounts (6 to 30 characters) and RustFS service accounts.
    The key name carries the store and the device making the key, for §13's
    reclaim: `coven-<store>-<device>-<call>`, with the first 24 hex digits of
    the store id, the device id's 16 and the call's first 16, 64 characters,
    an IAM user name's limit.
  - A store's listing returns every key held under a key name for that store,
    with the device the name carries: on IAM services, `ListUsers` (under
    `/coven/` where users carry a path; SeaweedFS keeps none) and each such
    user's `ListAccessKeys`, with the managed policy's ARN from
    `ListPolicies` on SeaweedFS; on GCS, the bucket project's service
    accounts, every page, and each matching account's HMAC keys; on R2, the
    account's tokens; on RustFS, the parent's service accounts. A failed or
    incomplete listing is an error, never a shorter list. On **2026-10-15**
    the Versity Gateway 1.9.0 minter's live run listed the minted key under
    its store with its device and listed nothing after revocation; the other
    listings are checked against in-process fakes of their APIs.
  - Before delivery the key is checked from the member's side. Under it, an
    object in `<store>/.coven-check/<run>/` is created, refused a second time,
    read back and deleted, and that prefix is listed, with its multipart
    uploads. Bucket versioning is read as setup reads it.
    Where keys reach a
    prefix, a listing of the location's base prefix, outside every store,
    must be refused; where they reach a whole bucket, the account's bucket
    listing must be. Otherwise the check fails with
    `StorageFailure::KeyBeyondStore`. Authentication and permission
    failures, after which a retry finds its run's object already created,
    and admin calls answered NotFound for a principal the service does not
    yet see (a mint repeats under its names) are retried each second within
    the service's retry window: 60
    seconds for AWS IAM, Ceph RGW and Versity Gateway, and 30 for SeaweedFS,
    whose gateways read identity changes asynchronously. The 60-second window
    is a retry limit, not a verified provider propagation guarantee; a key
    still refused at the limit fails the operation.
  - A failed mint or check rolls back what the call's names created and
    reports the failure, with the rollback's own failure if that fails too.
    Admin access is held only by the operation that needs it and released
    once its key is checked (a replacement's once it ends, §13) or its call
    fails
    ([`released_makes_nothing`, `check_keeps_admin`](proofs/storage/CovenStorage/AdminAccess.lean));
    after a restart
    the step waits with `PendingReason::AdminAccessRequired` until a retry
    supplies admin access again.
- Sealed key copies occupy sequence slots with clear routing (§11, D9).

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- A *row change* is one row's insert, update or delete within a write.
- SQLite's pre-update hook records the row changes each write makes.
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
  - So local writes need no store key, including before storage has been
    set up ([E1](api.md#e1-opening)).
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    read_through: 20
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

A *slot* is one create-once object in the store's sequence:
`<store>/seq/1`, `seq/2`, `seq/3`, and so on.
A *claim* is an attempt to create the next slot.
The slot holds an admin entry, writes, a key copy, or a snapshot announcement.

- A device applies slots in order. It claims n+1 only after applying 1 through n.
  [One sync pass](sync-pass.md) gives the read, apply, claim and effects order.
- Applying a slot commits its changes and applied slot number in one transaction.
  Failure leaves both at the preceding slot; SQLite atomicity is assumed (§2).
- In the model, each followed audience remains at the state of a sequence prefix on
  storage satisfying §15's coverage and snapshot-body rules
  ([`read_ok`](proofs/sequence/CovenSequence/Reader.lean),
  [`dev_ok`](proofs/sequence/CovenSequence/Device.lean)).
- Parts outside this member's audiences need not be opened ([§14.4](#144-writes)).
  A readable part missing its key stops the whole slot.
- Writes cut off by removal or key replacement are consumed without effects
  and record `WritesRefused` ([§10](#10-device-identity)). They do not stop later slots.
- A write keeps its `(device, number)` identity from local commit.
  Numbers order that device's writes; they are not storage paths.
- Each pass claims at most one writes slot, containing its device's queued,
  sendable writes in number order. App writes committed after capture wait for the next pass.
- A schema raise can leave later writes waiting ([§17.1](#171-host-application)).
  A pass with no sendable writes claims no writes slot.
- A writes slot has no size cap. Large slots upload in parts.
  Each individual plaintext write must fit SQLite's 1 GB value limit; otherwise commit returns `DbError::TooLarge`.
- Each write records `read_through`: the last applied slot when it was authored.
  Its own earlier writes are implicit reads. Catch-up never changes this record.
  The model preserves the authored read and every queued original
  ([`authored_readThrough`, `catchup_preserves_originals`, `own_predecessor_is_read`](proofs/sequence/CovenSequence/Causality.lean)).
  Given publication after the authored read, own writes published in number order,
  and nondecreasing authored reads, slot order and within-slot write order consume
  every published cause first; authored pasts are transitive
  ([`ordered_application_causal`, `past_transitive`](proofs/sequence/CovenSequence/Causality.lean)).
  Receivers agreeing on a cause's slot agree whether it is in that past;
  later publication adds no intervening remote write to it
  ([`receivers_agree`, `intervening_write_not_read`](proofs/sequence/CovenSequence/Causality.lean)).
- Before first sending a write, record its format and sealing keys.
  [§11](#11-keys) chooses the keys; [D9](format.md#d9-sealed-objects) fixes its signed inner bytes across retries.
- Remove queued writes only when their claim is settled and their consumed
  state commits. Apply the remaining local queue over that state without applying any write twice.

### Stuck devices

A slot that cannot apply stops the device entirely until it can apply that slot:
it applies nothing after it, reloads no audience from the floor, and sends nothing,
including file parts, operation retries and provider changes. It reports
`UpdateRequired`, `KeyUnavailable`, the read failure, or a `Refused` reason.
The local app can still read its data and commit eligible writes.
The model proves that an already-stopped device takes no apply or reload step
([`stopped_stays`](proofs/sequence/CovenSequence/Device.lean)); sends and recovery
from the blocking cause are outside that model.

### Settling a claim

A *journal* is the device's durable record of unfinished work.
Each device uses one increasing claim counter for every kind of slot: writes,
admin entries, key copies and announcements. The claim number starts at one
and is sealed in the slot under the store key, separately from write numbers.
Before each create, the journal records the claim number, slot path and exact bytes.
An operation keeps this record in its journal; a writes queue keeps it with its batch.

- A device settles its current claim before starting another, across all its
  queues and operations. Contention retries keep that claim number; a new
  claim takes the next number. Never reuse a settled number for different work.
- On reopening or retrying, settle this claim before validating again, undoing,
  or trying another slot. Read its recorded slot and compare all bytes;
  forgetting the claim can land an operation twice
  ([`forgetful_lands_twice`](proofs/sequence/CovenSequence/Operation.lean)).
- Equal bytes mean the claim landed, even if the provider replied `AlreadyExists`.
  Different bytes signed by another device mean contention; signed by this
  device id, they mean another copy uses it (§10). A failed read leaves the claim unsettled.
  Skipping the comparison can duplicate a batch
  ([`no_compare_lands_twice`](proofs/sequence/CovenSequence/Claim.lean)).
- If the slot is absent and the floor has not passed it, retry the same
  path with the same bytes. Absence does not cancel a request still in flight.
- After a create lands, read the floor before confirming the claim.
  A slot still at or above that floor holds the genuine bytes
  ([`claim_floor_check`](proofs/sequence/CovenSequence/Retention.lean)).
  If the floor has passed its slot, recreated bytes cannot confirm it
  ([`stale_create_wins_deleted_slot`](proofs/sequence/CovenSequence/Claim.lean)).
- When the floor has passed the recorded slot, compare its recorded claim number c
  with the authenticated floor snapshot's last included claim number L and that
  slot's digest ([§15](#15-snapshots)). This settles every slot kind, valid or refused:
  - L below c: the claim did not land;
  - L equal to c with the digest of the recorded bytes: it landed;
  - L above c, or equal with another digest: another copy uses this device id;
    report `SyncError::DeviceIdInUse` and stop sending.
- The number alone is not enough: two copies of one id can claim with the same
  number, and the loser would take the winner's slot for its own
  ([`settle_correct'`, `settle_from_floor`, `copies_fool_number_only`](proofs/sequence/CovenSequence/Settle.lean)).
- Settling from the state fails: later writes or admin entries can erase a
  landed claim's effects ([`admin_state_cannot_settle`, `state_alone_cannot_settle`](proofs/sequence/CovenSequence/Settle.lean)).
- If the number has not reached the claim, read forward and validate again
  before claiming at the current end with that claim number.
- Any bytes recreated below the floor are deleted as recorded cleanup.
  Failure retains that obligation; those bytes never change the sequence's meaning.
- When another claim won, apply the new slots and validate the waiting work again.
  A new slot needs a new outer envelope; its inner writes stay unchanged.

With this journal ordering and floor coverage, settlement distinguishes a landed
claim, one absent from the sequence, and an attempt whose slot is still empty
([`settle_correct`](proofs/sequence/CovenSequence/Settle.lean)). Once an occupied
attempt is settled, retaining its claim or successfully claiming again after
`notLanded` leaves exactly one slot for that claim
([`lands_once_never_lost`](proofs/sequence/CovenSequence/Settle.lean)). This proves
no bound on contention or time to success.

Ana and Ben try slot 41. Ana's reply is lost, then her retry says `AlreadyExists`.
She reads her bytes at 41 and checks the floor. Ben reads different bytes,
applies Ana's slot, and tries 42.

## 7. Order

Two mechanisms order writes:

- causality decides when a device may apply a write;
- timestamps decide which write a cell keeps.

### 7.1 Causality

- A write's past is every write in slots at or below its `read_through`,
  plus its own earlier writes. Slot zero means no published history read.
  Claiming a later slot does not enlarge that authored past.
- Writes in one slot are in the author's write-number order. Every cause
  is in an earlier slot or is an earlier write of that same batch.
- Authenticate each write with its device owner's signing key in the state
  at its `read_through`. The first device’s offline writes made before
  setup have `read_through = 0` and use slot 1’s creation binding; no other
  device can author at zero. That state must authorize the device, member and
  each circle part. Missing evidence waits; an invalid signature or authority
  refuses the slot. §10 separately consumes writes cut off by an earlier
  removal or member-key replacement, without exposing their data as losses.
- A consumed cause need not have retained effects. Schema-excluded changes,
  reset-ignored parts and refused retirement writes keep their verdicts;
  actual data sources decide dependent losses (§8).
- Ana creates note 43 in slot 20. Ben reads through 20, then queues an
  attachment to it. Its later slot follows 20 on every device.
- Ben and Carol both edit while their `read_through` is 20. Their writes
  remain concurrent even if Ben claims 21 and Carol claims 30: neither edit
  read the other. Their cell values still compete by timestamp (§7.2).
- App commits are visible locally before their writes claim slots. Reading slots in order
  combines that published prefix with the local queue; it never pretends
  an offline write saw intervening slots. Once the same writes are published,
  devices use the same slot order and authored pasts.

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
  is intact and the journal check in §10 finds no evidence of another copy.
- The stamping rule:
  - each device keeps the latest timestamp it has seen, from its own writes
    and every write it applies, saved on disk;
  - a write waiting for a required key or update is not seen until it applies;
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

- At 13:00 Ana's fast phone stamps write 4 at 13:01:00.000 #0 and claims
  slot 21. It changes “Grocery list” to “Groceries”, with `read_through = 20`.
- Ben reads slot 21 at 13:00:20. At 13:00:30 his phone stamps write 9 at
  13:01:00.000 #1 and queues “Weekly groceries”, with `read_through = 21`.
  He claims slot 22.
- Carol reads 21, then 22. Ben's counter makes his value win over Ana's,
  even though his wall clock was behind. An offline concurrent edit still
  competes by its timestamp, not by its eventual slot.

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
    merged state and applied admin entries ([§9](#9-members-and-roles)).
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
    dismissible losses, through conversion and snapshots. Snapshot
    coverage records consumed slots; it cannot turn a dropped source
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
  - deleted circles: a row in a circle whose deletion has been applied is taken
    out ([§14.7](#147-deleting-a-circle));
  - unique values: of two rows claiming one value, the later slot claim
    is taken out, since the first claim keeps it
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
  rows hidden by a circle-deletion slot.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Merging uses these internal tables:
  - `_coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number;
    - its containing slot and original `read_through`; own earlier writes
      are implicit reads.
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
      with the smallest timestamp. This generation representative is not
      the first-slot creator retained for owner permission (§8.8).
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
  - A present row's displayed values are in the app's table. When a cell
    reads differently from what was written, a reference read as null
    (§8.4) or a generated column computed from merged inputs,
    `_coven_written_values` keeps the written value by cell until the two
    agree again.
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
    All losses use this record and one snapshot section. Keep required source evidence separately; dismissing one cannot change a source's verdict.
- `_coven_circle_deletions` names the entry deleting each circle. The applied
  admin state holds each audience's schema/reset slot and named snapshot.
  Snapshot metadata retains the historical facts needed for old writes.
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

- Carol's tablet goes offline at 12:30, having applied through slot 20,
  including Ana's write 3 and Ben's write 8.
- At 13:00 Ben sets the title to "Weekly groceries". His phone commits this
  as its write 9.
- At 14:00 Carol sets the title to "Shopping". Her tablet commits this as
  its write 2:

  ```
  carol-tablet, write 2, 2026-10-02 14:00:00.000 #0
    read_through: 20
    notes  row 42  update  title: "Grocery list" → "Shopping"
  signed with Carol's key
  ```

- At 14:30 the tablet comes back online and uploads the write record to
  `<store>/seq/23`, after reading slots 21 and 22. Her authored past stays 20.
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
    read_through: 20
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
                 read_through: 20
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
      read_through: 20
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
- A foreign key of a synced table that is one generated column is refused
  on open and after migrating with `SchemaError::GeneratedForeignKey`,
  naming the table and foreign key.
  - Coven merges the column's inputs cell by cell. Ana setting `a` while Ben
    sets `b` would make `ref AS (a || b)` name a parent neither write named,
    and the foreign key would fail on every device
    ([`unwritten_reference`](proofs/merge/CovenMerge/GeneratedReference.lean)).
  - A key of several columns captures its generated components' inputs with
    it, so every reference a device shows is a tuple one write set
    ([`accepted_reference_written`](proofs/merge/CovenMerge/GeneratedReference.lean),
    [B19](proofs/merge.md#b19-foreign-keys-on-generated-columns)).
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
- Entry-caused removals use the same local foreign-key actions. A final
  circle deletion cannot later restore those local children; explicit app
  writes still follow the ordinary local schema rules.
- A synced row coven deletes or takes out can have children that only this
  device has, in local tables, so a local foreign key must never stop it:
  - coven refuses, checked when the database opens and after migrating, a
    foreign key from a local table into a synced table, or into a local
    table one of these reaches, unless its delete action is cascade, or
    set null on a column that allows NULL;
  - e.g. `pins(note_id REFERENCES notes ON DELETE RESTRICT)` is refused:
    Ben's phone, holding a pin, couldn't take out a note that Ana's phone
    could, and the two would differ.
- Coven applies another device's change to a synced non-key column as an
  `UPDATE`, so a local foreign key must follow that update too:
  - coven refuses, checked when the database opens and after migrating, a
    foreign key from a local table into a synced table's non-key column
    unless its update action is cascade or set null, with
    `SchemaError::LocalChildUpdate`, naming the table and column. A local
    column such a key rewrites passes the update on, so a local key into
    that column must follow it the same way;
  - e.g. `pins(code REFERENCES parents(code) ON DELETE CASCADE)` is
    refused: its update action is NO ACTION by default. Ana's phone, holding
    a pin on code `'a'`, could never apply Ben's renaming it to `'b'`: every
    attempt fails SQLite's foreign-key check at commit and rolls back to the
    same state ([`stuck_forever`](proofs/merge/CovenMerge/LocalUpdateAction.lean)).
    Cascade and set null keep every local reference valid along the chain
    ([`chain_holds`](proofs/merge/CovenMerge/LocalUpdateAction.lean),
    [B20](proofs/merge.md#b20-local-keys-into-values-coven-updates));
  - a key column changes by a delete and an insert (§8.5), which the delete
    rule above covers.
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
    NULL in it names no row;
  - a column of a synced primary key, unique constraint or unique index, or
    of a foreign key from or into a synced table, that declares a collation
    other than BINARY, and a unique index term with one
    ([Appendix A.10](#a10-collations-on-keys)).
    - Coven compares key and unique values by their bytes
      ([D3](format.md#d3-ordered-keys)), as the merge model compares rows
      ([Appendix B2](proofs/merge.md#b2-terms)); under `COLLATE NOCASE` SQLite would hold "Urgent" and
      "urgent" as one row that coven treats as two.
    - Other columns may use any collation.
- Either kind can span several columns.
  - E.g. `note_tags(note_id, tag_id)` is a shared key.
  - Ana and Ben, both offline, each tag note 42 "urgent", and make one row.
  - An independent key over several columns needs a UUID in one of them.
  - Bind independent UUID keys with `UuidText` (E3). Inserts still refuse
    text outside canonical lowercase UUIDv4/v7; opening never scans rows.
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
- Among merged live rows claiming one value after the first pass of other
  removal rules, the lowest counted slot claim keeps it.
  Within one slot, the lower write number wins; within one write, the lower primary key wins.
- Given merged presence, constraint values, counted input setters and distinct
  primary-key ranks, the model selects that claim; sound readers of one audience
  at the same position select the same winner
  ([`first_live_claim`, `winner_counted_setter`, `within_slot`, `readers_agree`](proofs/sequence/CovenSequence/LiveClaims.lean)).
- Timestamps choose the cell values before uniqueness is judged.
- Local writes without slots rank after claimed slots, in local write order.
  A locally created row can therefore lose its unique value before claiming a slot.
- The other row is taken out, whether its write inserted it or edited it
  to claim the value, and recorded as lost.
  - A change to its own value that ends the conflict clears that reason.
  - When the winner releases the value, it goes to the earliest surviving
    live claimant, including an older loser. The first claim after release
    wins only when no earlier live claimant survives
    ([`deleted_releases`, `first_after_release`, `older_loser_returns`](proofs/sequence/CovenSequence/LiveClaims.lean)).
- E.g. note titles are unique, and Ana and Ben, both offline, each add a
  note titled "Groceries".
  - Ana's insert of note 45 claims slot 40; Ben's note 46 claims slot 41,
    even if he authored it earlier while offline.
  - Ben reads slot 40 before sending his note. His local note loses "Groceries"
    and is recorded in `_coven_lost`.
- A row's claim is the greatest `(slot, write number)` among the setters
  of the constraint's current inputs, when that complete value was established.
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
  - If these writes claim slots 50, 51 and 52, note 2's combined claim
    is 52 and note 1's is 50. Note 1 keeps the value.
- A winner taken out after unique values are judged doesn't bring its
  loser back ([§8](#8-merge)).
  - E.g. notes are unique by title, and a sub-note points at its parent
    note with cascade.

    ```
    09:00  note 1 is "Ideas"
    10:00  Ana adds note 2, "Plan", as a sub-note of note 1
    11:00  Ben, not having seen note 2, renames note 1 to "Plan"
    ```

  - If these writes claim in the shown order, note 2 keeps "Plan", and
    note 1 is taken out.
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
  - For owner permissions its targets are direct writes, even when a
    foreign-key action fires it, unless a step equals a foreign-key action's
    exact effect (§8.8).
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
      read_through: 20
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
  - Generation-zero creations of a shared key select the lowest claimed
    slot, then write number inside that slot, before permission filtering.
    Later claims never reselect an established owner. Local unclaimed
    creations are provisional; when a published creation already owns that
    key, queued changes are checked against it before their effects count.
    Given the counted generation-zero creations and their authenticated members,
    the model selects by slot and write number when no creator is retained
    ([`selected_creation`, `lower_write_number_wins`](proofs/sequence/CovenSequence/Owners.lean)).
    Later claims and modeled updates, deletion, reinsertion and audience moves
    preserve that creator and original write
    ([`later_claim_preserves_creator`, `move_preserves_owner`, `retained_creator`](proofs/sequence/CovenSequence/Owners.lean)).
  - Retain the creating write's authenticated identity through deletion,
    loss dismissal, schema conversion and snapshots. Renaming a table or
    key does not make the migration's author the row's creator.
- Only the owner may directly update or delete an owned row. An insert over
  an existing or removed shared key is an update for this permission.
  - Direct targets include the write's own statements and its shared
    triggers. Check every direct target even if its effects combine with a
    foreign-key action on the same row. A trigger cannot borrow permission
    from the row that fired it.
  - At commit, the author checks its own write as receivers do (below). A
    change they would refuse refuses the whole local transaction with
    `DbError::NotOwner` or `DbError::InvalidForeignKeyEffect`, leaving its
    rows and upload queue unchanged.
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
  - A step is a foreign-key action exactly when it equals, with the parent
    each written reference names, the schema's consequence of an earlier
    step of the write through a reference its row holds at that moment: the
    authoring view replayed through the write's earlier steps. Every other
    step is direct. A trigger's step that equals such a consequence, made
    before SQLite's action reaches the row, is that action's exact effect and
    counts as it ([B18](proofs/merge.md#b18-owner-tables-and-row-authenticity)).
  - An action also records the columns another reference shares with its
    own, and that reference's parent: the one it held, when the action
    leaves its values as they were, or else the row its new values key, as
    the write's earlier steps leave it. When the write never touched that
    row, or a recorded reference's parent may have ended before the write,
    the step depends on rows of the authoring view that no step carries, and
    every device refuses it (B18 `Overlapping`).
  - Match the exact row, generation, operation and every changed column and
    value, including all components of a compound key. An action's delete
    shows the values its replaced setters wrote (D5). Several actions on
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
  authorization and loss recording at the stated SQL extraction boundary.
  Ownership grants no additional schema restrictions.

## 9. Members and roles

- A member is a person with a random stable `MemberId` and current signing
  and sealing keys. Devices and circles name the id.
- An admin entry's `EntryId` is its slot number.
  [D6](format.md#d6-admin-entries) lists the actions and their fields.
- Validate an entry against the state after every preceding slot.
  Equal preceding prefixes give equal validity decisions
  ([`valid_prefix`](proofs/sequence/CovenSequence/Converge.lean)).
  Loading a snapshot and applying its suffix establishes that same state.
- An applied entry is final. Later entries change state from their own slots onward.
- With membership and role changes admitted only from an active admin in the
  preceding state, and effects leaving no admin refused, every prefix after
  a creation binding containing an admin retains one
  ([`applied_membership_authorized`, `last_admin_refused`, `every_prefix_has_admin`](proofs/sequence/CovenSequence/Authority.lean)).
  Other admin actions retain their supplied authority policies in this model.
- Slot 1 creates the store and registers its first device and admin.
  Its supplied public key verifies the signature; its store id must match the path.
- The creator holds the first store key and claims its sealed copy next.
  Setup succeeds after both slots are confirmed. A different creation returns `StorageFailure::StoreExists`.
- Other entries use their member's current signing key before that slot.
  Their authoring device must be active, or be the fresh device the entry registers.
- Every admin action uses [§18](#18-operations), including role changes, renames and removals.
  Without storage or an applicable sequence, a new call fails before creating anything.
- Several members may be admins. Only admins add or remove members and change roles.
- Each member adds and removes their own devices. Admins may remove any device.
  Added devices always have fresh ids.
- Members replace their own keys; admins may replace another member's keys.
  Both new keys must be unused. Replacement preserves the member's id, role, circles and devices.
- Members set their own access. Admins may set another member's S3 access.
  Provider-admin credentials alone grant no store authority.
- Any member may create a circle. Only its members manage it or rotate its key.
- A target must exist. Removing a member or changing a role must leave an admin.
  The provider's store owner cannot be removed ([§13](#13-removing-members-and-devices)).
- An already-satisfied request completes without another entry.
  Adding an active member must repeat its exact keys, role and access.
- A removed member id is never reused. Inviting that person again gives them
  a fresh id and keys.
- Key replacement requires the operation's expected old pair still to be current.
  Another completed replacement requires a new call.
- Snapshots preserve historical bindings, removed identities and used public keys.
  These authenticate old writes and prevent identity reuse.
- Creation supplies the store id and name. Bootstrap settings must match;
  a mismatch returns `DbError::StoreSettingsMismatch`.
- Applying an entry commits its state and required work together.
  A failed transaction advances nothing. Subscriptions expose only committed results.

Ana and Ben both try to remove each other. Ana claims 41 first.
Ben reads it and loses authority; his call undoes its work and fails.
No removal of Ana enters the sequence.

## 10. Device identity

- A device is one installation with its own id, belonging to one member.
  Its id also lives in custody that backups do not copy.
- Missing or different custody refuses opening without changing the database.
- When reading a new slot, check whether it is signed by this device.
  If this device's journal never attempted that path, report `SyncError::DeviceIdInUse` and stop sending.
  Starting from empty storage, with every create recorded and the journal
  retained, the model proves that an unattempted signed slot implies another copy
  ([`foreign_slot_means_copy`](proofs/sequence/CovenSequence/Settle.lean)).
- Different bytes at an attempted slot are contention when another device signed
  them. Signed by this device id, they mean another copy uses it: report
  `DeviceIdInUse` and stop sending ([§6](#settling-a-claim)).
- Slot order controls sending ([§6](#6-syncing-writes)); no elapsed-time permission is required.
  A removed device or an installation holding replaced member keys cannot send.
- Creation and invite-bound join requests have their bootstrap rules (§9, §12).
  They do not permit ordinary sends before the device has applied the sequence.
- A writes slot after its device's applied removal or replacement counts for nothing.
  The same applies after its member's removal or replacement of its signing key.
- Authenticate the original author, consume the slot, and record `WritesRefused`
  with the deciding entry. Those writes create neither data effects nor losses.
  The model proves refusal with unchanged rows and no new unique-value or owner
  claim, while still recording the claim number
  ([`cutoff`](proofs/sequence/CovenSequence/Cutoff.lean)).
- Eligible changes using refused data follow [§8](#8-merge)'s source rule.
  Writes signed by the member's new key can count after replacement.
- Writes claimed before removal keep their admission.
  Circle removal alone follows [§14.6](#146-leaving-a-circle).
- Ana's old phone sends write 8 while its replacement claims 70.
  Write 8 counts at slot 69; at 71 it is consumed and refused.
- Applying this device's or member's removal stops syncing and tells the app.
  Device replacement reports `DeviceIdInUse`; member-key replacement reports `MemberKeysReplaced`.
- Restoring with current member keys registers a fresh device.
  It does not rescue the old installation's queue.
- Self-replacement pauses app writes with `MemberKeysChanging` while it confirms
  queued writes, claims the new binding, and installs custody and the restore code (§18).
- `reset_device` discards the local database and unsent work, creates a fresh
  id, loads storage, and registers that id as replacing the old one.
- Only the same member may replace its device id.
  Separate new devices may replace the same old id; each keeps its fresh identity.
- A failed reset resumes with its recorded new id.
  Ordinary opening refuses the installation until reset finishes.
- Carry readable `DeleteAccessKey` obligations, owed account revocations and open
  invites as owed cleanup into the new database with its reset notice before opening it.
  A database too damaged to read supplies no records; orphan keys follow §13.
- Commit the `DeviceReset` notice with old and new ids before returning the handle.
  The notice survives reopening and tells the app that unsent edits were discarded.
- An unconnected reset creates an empty database with a fresh id.
  Competing first setups still contend for slot 1 (§9).
- A backup may continue with intact custody unless history shows another copy,
  removal or replacement. This detects only the evidence described above, not every clone.
- Content-bound nonces protect changed plaintext even when a backup forgot an attempt (§11.1).
- Outside the sequence, an occupied immutable path requires complete-byte equality
  with its recorded attempt. A mismatch at an own artifact path stops that attempt.
- A failed comparison read remains pending. Metadata alone never proves equality.
  Provider access cut-off remains separate ([§13](#13-removing-members-and-devices)).

## 11. Keys

- Store keys encrypt store data and metadata; circle keys encrypt circle data.
  Member signing/sealing pairs and provider credentials have separate purposes.
- The store key seals slot device/claim numbers and inner write part audiences.
  Its exposure reveals those headers, its store data and admin contents;
  circle parts and recipient boxes still need their own keys.
- Each file has its own key inside its encrypted row ([§16](#16-files)).
- Creation and rotation introduce an audience key by random id and SHA-256
  of its exact 32 bytes. These fields stay encrypted; removals carry no keys.
- A key copy occupies a slot. Clear routing names its audience, key id,
  recipient member and exact sealing key ([D9](format.md#d9-sealed-objects)).
- Open a required copy with the private key its routing names.
  If its introduction is present, check the key against it; a mismatch blocks the slot.
- Without its introduction, keep the opened key tentative and apply the copy.
  The later introduction confirms it before use, as described below.
- A copy to someone else need not be opened.
  Its signed routing still records whom the key reached.
- An operation claims required copies before introducing the key.
  The copies' outer encryption uses an accepted store key; the new key may seal its introduction.
- Creation is the exception: slot 1 establishes the first binding, followed by
  the creator's copy (§9).
- Before introduction, an opened key may only verify that introduction.
  It cannot encrypt app data yet.
- A copy exposes its key to its recipient, even if unopened.
  Copies to former members or retired credentials make the key unusable for new data.
- Check tentative recipients against the later binding or introduction.
  A candidate never bound still received the key; undo cannot erase that exposure.
- Before first sending data, use the newest usable audience key by introduction slot.
  It must be introduced, authorized, held, and unexposed in the applied prefix.
- If none is usable, acquire a replacement or rotate through §18.
  An admin outside a circle cannot rotate its key.
- Several rotations may follow each other. Introduced older keys remain readable;
  new data uses the newest usable key.
- Admin metadata may use the newest accepted store key held, even if exposed.
  Removal and rotation need no unexposed key.
- Retried inner writes keep the keys fixed on their first attempt (§6).
  Circle parts authored before leaving follow [§14.6](#146-leaving-a-circle).
- Key replacement retires the preceding sealing credential.
  Copies to it remain exposure even though the member's id stays active.
  Device removal alone does not retire the member's shared keys (§13).
- Share historical keys with current audience members whose credential lacks a copy.
  A retry does not require another copy already present in the sequence.
- Ana and Ben may both supply K to Carol in different slots.
  Both copies remain readable and record the same recipient.
- A new device first opens copies for its credential from the floor or sequence.
  It verifies introductions and membership before loading data (§12, §15).
- Once Ana applies Ben's removal, her first attempts exclude keys known to have
  reached Ben. An in-flight copy can still land after her read.
- That later copy exposes the key on the next read. Ana then needs another
  usable key; already attempted inner writes retain their bytes.
- Provider cut-off prevents new downloads, not opening ciphertext already held (§13).
- Private credentials and audience keys stay in custody.
  Keep historical private credentials needed for old boxes, or supply copies to the current credential.
- Self-replacement keeps the new private pair before claiming it.
  Failure retains both pairs and the recorded progress for retry.
  It completes custody and the restore code before reporting success (§18).

- Creation initializes the member in the chosen custody (OS keychain by default)
  before publishing the store.
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
- Coven encrypts with XChaCha20-Poly1305.
  - Each object's storage path is bound into its authentication, so the
    provider can't swap one object for another.
  - Each chunk of a file binds its index; each chunk of a write or a
    snapshot binds its section and its index: a write's header is
    section 0 and its part i is section i + 1, and a snapshot is one
    section.
- A slot's signature covers its path and a SHA-256 hash of every preceding
  byte. Inner writes bind their stable store/device/write identity instead
  (D9), so contention can change the slot without changing authored bytes.
- Nonces:
  - for a file's chunks, the chunk's index, which never repeats under the
    file's own key, so retrying an upload sends the same bytes;
  - for inner writes and sequence slots, the first 24 bytes of HMAC-SHA256
    under the encryption key, over a context binding the path or inner identity,
    cleartext prefix, section, chunk index and SHA-256 of that chunk's
    plaintext ([D11](format.md#d11-keys-and-contexts)). Binding
    the prefix also separates changed authentication data. Rollback and
    copied identities need no unique-path assumption for this separation;
    it still relies on SHA-256 and the truncated HMAC resisting collisions;
  - for everything else, random.
- A key is sealed to a member with an anonymous sealed box: X25519 with
  XChaCha20-Poly1305.
- Keys for each purpose are derived from the store key, or a circle's key,
  with HKDF-SHA256 and separate labels for encryption and authorship
  ([D11](format.md#d11-keys-and-contexts)).
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
  password manager, and enters it on the new device. Dropbox and OneDrive
  likewise need a sign-in to the person's provider account.
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
- Where storage needs a sign-in, such as Dropbox, the new device
  asks coven to sign in to the person's own account first, through the builder's
  presenter. Restore and join use coven's held tokens. Restore codes never
  carry OAuth tokens.
- The new device reads the floor and bootstrap copies, loads the snapshot
  and applies the retained suffix. The supplied id and pair must match an
  active current binding. An old code returns `MemberKeysReplaced`; it cannot
  bind its keys back. Register the fresh device through §18's claim loop,
  then finish loading and publish the handle. Interrupted claims settle their
  recorded destination before retrying or cancelling.
- Setup and member-key replacement return the restore code; `restore_code()`
  shows it again. Keeping or confirming it is app UX, with no pending record.
- On Apple, coven refreshes the iCloud Keychain code after setup, join or
  member-key replacement. S3 key changes update separate custody without
  changing the code ([E9](api.md#e9-members-and-devices)).

### 12.2 Adding a person

- On a provider that shares with an account, the admin who invites is
  the member whose account holds the store
  ([§4](#4-storage-providers-and-access)).
- An admin adds a person with an *invite*, a code shown as a QR code,
  holding:
  - the store's id, name and location;
  - the invite's id, the chosen stable member id, a one-time *invite secret*, the initial store key id
    from creation, the approving device's id and the latest applied slot
    when making the invite;
  - on S3, an access key made for the new person using the admin-access or
    guided console path in §4.
- The admin fixes the member id before showing the invite. For a new person
  coven generates a fresh id and the admin chooses a role. For recovery the
  admin selects an existing active member; its role and circle memberships
  are preserved. The requester cannot select a different replacement target.
- E.g. Ana adds Carol, on Dropbox:
  1. Ana enters Carol's Dropbox account email and picks her role; her phone
     shares the store's folder with that account and shows the invite.
  2. Carol's phone scans it, signs in to her own Dropbox account, mounts
     the shared folder, makes Carol's member keys, and writes a join
     request to storage, holding the invite id, its chosen member id, both
     new public keys and her device's name.
  3. Ana's phone shows the request, and Ana approves it.
  4. Ana's phone seals every store key it holds to Carol's new sealing key
     ([§11](#11-keys)), then publishes an "add member Carol" admin entry.
     - Every key, so Carol reads a late write made under an older one.
  5. Carol's phone opens the store key, publishes its "add device" entry, and
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
- Carol starts after the invite's slot, inspecting sequence kinds and
  key-copy routing without advancing an applied cursor. She retains bytes
  and her scan position, stops at a miss, and uses the shared backoff.
  Approval claims copies for her exact credential before its binding entry.
  It supplies every historical store key and, for replacement, held keys of
  her circles. Missing prerequisite keys block approval.
- The pre-membership scan can inspect routing before holding a store key;
  it is not sequence application or sending authority. Once she has her
  keys, Carol authenticates the matching approving device's binding, loads
  the floor's snapshot and applies its suffix in order. If retention
  passed her scan cursor, the floor's bootstrap evidence supplies its copies.
- Her own sealed keys plus the matching applied binding permit registration.
  Copies alone do not. If the request disappears, read forward again and
  check the floor before deciding decline or expiry: approval may have
  raced the preceding miss. A rejected approval is reported; it is not
  rescued by another device's unrelated binding.
- Before sending, Carol's phone keeps its new member keys and exact request
  bytes in its unpublished local directory, encrypted with the invite secret
  and bound to the store, invite and chosen member id. Restarting with the same invite and
  device name reuses them and the device id. These are not final key custody.
  - It records the create attempt before issuing it. If the attempt's
    reply is lost and neither a request nor sealed keys exist on retry, it
    treats the invite as settled. A request the provider refused was not
    stored; the retry sends it again. Recreating that request could revive an
    invite the admin already declined. An admin can issue another invite.
  - Provider permission failures reach the app even when revocation followed
    decline or expiry; an unreadable request is not an absent request.
- The entry adding a new Carol records how she reaches storage, her provider
  account or the id of the S3 key made for her and who holds it, so any admin
  can take it back later ([§13](#13-removing-members-and-devices)).
- Rebinding an existing id changes no access record or device id. The new
  installation registers its own fresh device after the binding is claimed.
  Any changed provider access is separate work under §13, with its own
  recorded access entry; a key replacement never implies provider cut-off.
- Once the request is approved or declined, or the invite expires, Ana's
  device deletes the request object.
- Resetting the inviting device carries its unfinished invites into the new
  database for the same grant revocation and request deletion as expiry,
  preserving active members' retained access (§13).
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
    writes claimed after replacement are consumed and refused (§10).
  - A removed member cannot be restored by replacing keys. Adding a new
    person instead uses a fresh id and the ordinary admission rules.
- Losing every member's devices and restore codes loses the store for good,
  since everything is encrypted.

## 13. Removing members and devices

- A removal is an admin operation whose claimed entry takes effect at its
  slot. Later writes under the removed device, member or replaced credential
  are consumed and refused (§10). Already claimed writes keep their admission.
- Removing a member removes their devices and their circle memberships.
  A circle left empty is deleted at this slot. No key or circle list is
  carried in the removal: the preceding state supplies it.
- Applying removals or access changes records provider work in the same
  transaction. That is the one scheduling path; the initiating call does
  not separately revoke access. `remove_member` returns after its entry
  applies. Pending work describes provider progress, including owner waits.
- Provider sharing follows active members and open invites. Only the account
  owner changes sharing; each owner's device serializes its requests and
  checks the latest intention before the next. If a later entry changes the
  intention while a request runs, finish it and perform the newly required
  request before reporting completion (§4).
- Revoke only grants exclusive to the account being removed. Parent, public,
  group, unidentified and owner grants remain reported as `AccessRemains`.
  An account still used by an active member or open invite stays shared.
  The member whose provider account holds the store cannot be removed:
  return `SyncError::StoreOwner` before claiming anything.
- Providers cannot generally cut off one device alone:
  - Dropbox and OneDrive: the person removes the app's account access and
    signs in again on retained devices;
  - iCloud: the person removes the lost device from their Apple account;
  - S3: an admin with operation-scoped provider access makes and checks a
    fresh member key, delivers it sealed to that member, claims set-access,
    then deletes the superseded key. Guided console steps remain available.
- S3 replacement records every artifact before creation (§18). Its delivery
  is `<store>/access-keys/<member>/<attempt>` (D9). A device installs it only
  when the same author's applied set-access entry names it as current.
  A device that missed delivery before old-key deletion must receive the
  new key from the person. The restore code is unchanged.
- On a claim collision, revalidate. If the target or authority is gone,
  undo the new key and delivery; preserve the old current key. Failed undo
  keeps the journal and both causes visible. A claimed set-access entry is
  final, and later access changes queue its superseded key for deletion.
- Every distinct S3 key no longer current becomes `DeleteAccessKey` work.
  Removal includes every recorded access for that member; ending an invite
  includes its new grants. A failed unclaimed operation includes artifacts
  its journal owns. Do not delete an active member's still-current key when
  undoing a replacement invite.
- A deletion record clears only after deleting that exact id and reading it
  absent, or an explicit `confirm_access_key_deleted` call. A successful
  delete reply, elapsed time, failed read or another key's absence cannot
  clear it. Keep confirmation by key id across reopening, so retries cannot
  restore the notice. Console-made keys require console deletion.
- Keys with no entry can outlive a lost database or lost mint reply. Their
  provider names carry the store and minting device (§4). An admin holding
  provider access lists them after that device's retirement is applied,
  and queues keys neither recorded in the sequence or store snapshot nor current.
  Later entries by the retired device cannot make such a key current.
  A live device's unrecorded key remains protected by its operation.
  The ordinary file-folder orphan scan still requires confirmed provider
  cut-off, including in-flight uploads (§16.5).
- Member-key replacement changes neither access records nor device ids.
  Old-credential writes follow the slot cut-off; provider credentials and
  exposed audience keys have their own recorded work. It does not certify
  a file folder closed.
- Ana removes Carol from the store. Ben still belongs to Gifts with Carol;
  his pass rotates Gifts before new data uses it. Ana is outside Gifts and
  cannot make that key. If Ben is offline, Gifts waits for a member to act.
- Ben removes Dan while Ana, the folder owner, is offline. Ben's call
  returns after its claim applies; his pending list names the owner wait.
  Ana's next applicable pass performs the revocation.
- Removal changes no already obtained ciphertext or private key. Confirmed
  storage cut-off prevents further reads and publication; until then the
  sequence's authorization and cut-off rules still apply on every receiver.

## 14. Audiences

- A *circle* is a group of members inside a store who share rows the other
  members can't read ([§14.3](#143-circles)).
- Every synced row has an *audience*: the store, or one circle.
  - A row in the store reaches every member's devices.
  - A row in a circle reaches only that circle's members' devices.
  - E.g. a household's notes are the store's, and each person pins notes
    in a circle of their own.
- The app declares, for each synced table, how its rows get their audience:
  - a *root* names a non-NULL audience column. Binding `Audience` or `CircleId`
    writes canonical text: `store` or a lowercase hyphenated circle UUID.
    Other text spellings, including uppercase, fail with `InvalidAudience`;
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

- Circles are made, and members added to and removed from them, by admin entries
  under their authorization rules ([§9](#9-members-and-roles)).
- Any member of the store can make a circle, and is its first member.
- A circle's own members rename it, add and remove its members, and delete
  it ([§14.7](#147-deleting-a-circle)); an admin outside the circle can't.
- Renames take effect in slot order. The latest valid rename supplies the
  name. A rename after deletion fails validation with the deleted target.
- Each circle has its own key, sealed to each member's current non-retired
  sealing key, with both member id and sealing key authenticated in each copy,
  like the store key ([§11](#11-keys)).
  - Its sealed copies are sequence slots, with the circle, key id and
    recipient in the clear routing ([§11](#11-keys)).
  - It is replaced whenever someone leaves the circle.
  - Someone joining a circle gets its earlier keys too, so they can read its
    history.

### 14.4 Writes

- A circle's changes are parts of the same writes batch as store changes,
  each encrypted with its audience key. One slot can carry Ana's note and
  her private pin, with a signature over all encrypted parts.
- Apply all readable parts atomically with the slot. Consume parts outside
  this member's audience without opening them; a missing key for a readable
  part stops the slot. Not being a recipient is not a missing-key error.
- The applied admin state determines which parts this device may read;
  the write's authored view determines the author's permission. Every device
  opens the header with the store key and checks the full signature, including
  opaque parts. Its part audiences are hidden from the provider; among slot
  fields, only the kind and key-copy routing are clear, besides D9's framing.
- Adding a member to an existing circle requires its historical keys and
  a reload of parts missed while outside it. The overall applied slot is
  not evidence those parts were read. Load its announced snapshot and suffix
  to the common target (§15), then commit its data and position together.
  Until that commits, writes into it return `AudienceReloading`.
- A newly created circle has no earlier parts to reload. A deleted circle
  cannot return through reversal; creating another circle uses a fresh id.
- Ben's laptop consumed Gifts' parts without opening them after leaving.
  When added again, it loads that history before writing Gifts, while other
  audiences remain locally usable. A missing key or snapshot is reported.
- Authorized historical keys remain readable. Adding Ben again may require
  sharing keys introduced while he was outside Gifts; it does not make
  exposed keys usable for new first attempts (§11).
- All members can see that a circle exists, its members, who writes to it,
  when, and how much. Only its members read its contents.
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
  in a circle this member isn't in, or a circle absent from this device's
  applied membership.

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

- Ana removes Ben from Gifts with an admin operation. The slot changes its
  membership; remaining members rotate exposed keys before first sending
  new data (§11). Ben keeps the rows and keys he already had.
- Removing its last member deletes the circle at that slot. The same is
  true when a store removal leaves it empty. The preceding prefix determines
  this; a later operation cannot add a member to a deleted circle.
- A circle edit authored before Ben read its removal can still count.
  Circle removal does not retire his device or member. He may send those
  queued parts with the old circle key he already held, exposing no new
  data to himself. A separate device/member removal or member-key replacement
  applies the slot cut-off (§10).
- This relies on current members being trusted about their recorded reads
  (§2). Once Ben reads the removal, new writes into Gifts return
  `NotInCircle`; he no longer snapshots it.

### 14.7 Deleting a circle

- Any circle member can request its deletion. The operation claims one
  admin entry; it makes no row write and advances no row generation.
- From that slot, the deleted-circle rule hides its rows, including later
  eligible pre-deletion edits. Their losses name `DeletedCircle` and that
  entry. A new local write into it returns `DeletedCircle`.
- Ana queued note 9 before Ben's deletion claimed slot 50. If her member
  and device remain active, her later batch is consumed and note 9 is hidden
  with Gifts' other rows. The app can offer its values in another audience,
  preserving its key (§8). There is no later reversal of the deletion.
- Keep merge rows, losses and source evidence for arriving old writes and
  snapshot loading. Devices outside the circle see the entry, not values.
  Dismissing a loss still follows its ordinary write and owner rules.
- Applying deletion releases local original-source records no visible row
  can name again. Queued files retain their own sources and finish under
  §16.5. Retained losses, snapshots, writes and queue references continue to
  protect stored file bytes; deletion is not permission to discard them.
- Physical slot and snapshot deletion follows §15's coverage and grace,
  and file deletion requires §16.5's complete reference checks.

## 15. Snapshots

A *snapshot* records one audience after applying through a slot.
Its *coverage* is that slot number, including writes consumed without effects.
It holds rows, merge records, losses and the evidence needed for old writes.

- Store snapshots also hold signed admin entries, key copies and announcements.
  They preserve the history needed for membership, keys, schema changes and resets (D7).
- Each snapshot records, for every device, its greatest included claim number and
  the SHA-256 of that claim's slot bytes. On a tie, possible only with cloned device
  ids, keep the later slot's digest.
  Count every slot through coverage: writes, admin entries, key copies and
  announcements, valid or refused, including writes with opaque or reset-ignored parts.
- A device with no included claim has number zero and no digest. These numbers settle every
  kind of claim after slot deletion (§6); they do not describe surviving effects
  ([`claims_ge`](proofs/sequence/CovenSequence/Settle.lean)).
- A snapshot excludes local uploads, journals, pending records, file sources and caches.
  Reloading preserves them; a reset clears pending records only as §19.3 specifies.
- A device snapshots only audiences it can read.
  It captures applied slots; local writes without slots stay outside the snapshot.
- To make a snapshot available, journal it, upload its fixed bytes, then claim
  an announcement naming its path, coverage and purpose.
  A counted preparation announcement has its prepared body already in storage
  ([`announcement_uploaded`](proofs/sequence/CovenSequence/Preparations.lean)).
- An unannounced upload supplies no coverage. If its operation cannot finish,
  the journal deletes it and retains any cleanup failure.
- An announcement's coverage must not decrease that audience's announced coverage.
  Equal coverage selects the later announcement. There is no upload deadline.
- An ordinary snapshot must cover the last schema raise and reset before its announcement.
  Its schema and reset state must match the audience at that slot.
  The model proves both conditions for counted ordinary announcements
  ([`ordinary_covers_boundary`, `ordinary_matches_schema_reset`](proofs/sequence/CovenSequence/Preparations.lean)).
  For raises, the version rule rejects a stale capture that would strand a joiner
  ([`slot4_refused`, `stale_floor_strands_joiner`](proofs/sequence/CovenSequence/ReaderExamples.lean)).
- A prepared raise or reset snapshot is usable only after its matching admin entry.
  Until then, its announcement cannot authorize retention or ordinary reload.
  Starting with no preparations, the model requires an earlier matching counted
  announcement for activation, which makes its coverage usable
  ([`activation_requires_preparation`, `activated_is_usable`](proofs/sequence/CovenSequence/Preparations.lean)).
- A raise or reset snapshot must cover at least as much as every earlier
  announcement for that audience. It must also cover the preceding raise and reset slots.
- After contention, recapture if these coverage conditions no longer hold.
  For modeled raises and resets, these coverage rules let a sound reader with its key
  advance and reach the head without a retention hold, assuming honest snapshot
  bodies and retained raise/reset bodies
  ([`read_progress`, `reaches_head`](proofs/sequence/CovenSequence/Reader.lean)).
- Delete an abandoned preparation through its journal. An announcement whose
  preparation never takes effect remains history, but supplies no usable snapshot.
  The announcement alone changes no rows, schema or reset state
  ([`preparation_no_effect`, `abandoned_supplies_no_coverage`, `abandoned_not_usable`](proofs/sequence/CovenSequence/Preparations.lean)).
- Select the newest applicable snapshot for the audience's schema and reset.
  A missing key, required update or damaged body waits; do not choose an older snapshot to bypass it.
- Applying an announcement uses its own signed coverage and purpose and reads no
  body. Loading a snapshot checks the body against the announced digest, then its
  signature and prefix ([D9](format.md#d9-sealed-objects)). An undone preparation's
  deleted body therefore never blocks a reader that does not load it.
- Request a snapshot when new encoded parts exceed the last snapshot's size,
  or 1 MiB without one. Retention also requests missing coverage below that threshold.

### The floor and reloading

The *floor* is one signed record at `<store>/floor`.
It names the first kept slot and the snapshot it relies on for each audience.
Before a floor exists, the sequence starts at slot 1.

- Every named snapshot covers at least `first - 1`.
  Include the store and every circle whose deleted history still matters, even if the writer cannot open that circle.
- Snapshots may cover different slots. Load readable audiences through one target:
  the greatest selected coverage or required applied slot, whichever is later.
- Read the retained slots each audience needs after its snapshot.
  Commit all readable audiences together only after they reach that target.
- The floor carries original key-copy boxes needed to open its store key.
  Their routing is clear; snapshot references stay encrypted (D8, D9).
- A joiner opens its box, verifies the historical binding and floor signature,
  then loads the snapshots and applies the retained slots. Without a floor, bootstrap starts at slot 1.
- A device missing its next slot reads the floor.
  If the missing slot is below `first`, reload from the floor's snapshots.
- Applying a raise whose replay needs a slot below `first` reloads that audience
  from the floor's snapshot instead. That snapshot was announced after the raise,
  so it covers it; a reader below a floor snapshot's coverage may always reload
  ([`read_progress`](proofs/sequence/CovenSequence/Reader.lean)).
- A gap at or above `first`, before a known later slot, is damage.
  A failed floor read proves neither absence nor permission to skip history.
- A reader also reads the floor after the pass's slots and before committing them.
  If it has passed any unapplied slot read, discard those inputs and reload.
  A read followed by a floor check admits only genuine first bytes
  ([`floor_check_sound`](proofs/sequence/CovenSequence/Claim.lean)); omitting it
  admits the recreated-slot counterexample
  ([`stale_create_wins_deleted_slot`](proofs/sequence/CovenSequence/Claim.lean)).
- The floor takes effect when written, even while old slots remain in storage.
  Checked reads preserve the audience's prefix state
  ([`read_ok`](proofs/sequence/CovenSequence/Reader.lean)).
- Fix the reload target when loading begins. New writes or announcements do
  not restart it. If deletion removes needed slots, use the newer floor's snapshots.
  The floor's rules alone do not imply that its snapshot supersedes a reload's
  selected announcement ([`unchecked_reselection_goes_backwards`](proofs/sequence/CovenSequence/ReloadExamples.lean)).
- Reload commits rows, merge records, source verdicts and the applied slot together.
  Failure preserves the previously visible state.
- Ordinary reload permits local app writes throughout, then applies eligible queued
  writes over the loaded state. Their numbers, timestamps, schema and original reads stay unchanged.
  Installation uses the queue present at commit, including writes made while loading,
  and preserves even ineligible originals for publication
  ([`queued_originals_preserved`, `write_during_reload`](proofs/sequence/CovenSequence/QueueReload.lean)).
  With unique queued write numbers, each eligible original enters the rebuilding
  fold exactly once; consumed originals are excluded, and repeating installation
  rebuilds from the published base
  ([`queued_exactly_once`, `covered_not_reapplied`, `install_twice`](proofs/sequence/CovenSequence/QueueReload.lean)).
- Reset, breaking raise and newly readable circle reloads refuse writes into that
  audience with `AudienceReloading`. Other audiences stay locally writable.
  Ordinary reload admits commits to followed audiences; an affected audience's
  gate refuses a commit containing its part
  ([`ordinary_commit_allowed`, `affected_commit_refused`](proofs/sequence/CovenSequence/QueueReload.lean)).
- For a changed followed set, each audience, including a newly readable circle,
  must supply its own sound loaded prefix at the common target. Installation
  then rebuilds each from its eligible queued originals. Eligibility requires an
  unconsumed write, admission, an audience part and an authored read through its reset
  ([`reload_sound`, `new_circle_keeps_queue`](proofs/sequence/CovenSequence/QueueReload.lean)).
  On good storage, with the audiences' keys and a target between every selected
  floor snapshot's coverage and the head, all those prefixes can be loaded
  ([`audiences_can_load`](proofs/sequence/CovenSequence/QueueReload.lean)).

On storage satisfying the coverage rules and holding honest snapshot bodies,
a running device with a fixed set of followed audiences and all their keys can bring every reader to
the head ([`device_reaches_head`](proofs/sequence/CovenSequence/Device.lean)).
Loading a floor snapshot and its suffix reaches the full-prefix state;
sound readers at the same position agree
([`joiner_reaches_head`, `readers_agree`](proofs/sequence/CovenSequence/Reader.lean)).

Ben returns after a year. The floor starts at 501; his next slot was 41.
He loads the floor's snapshots and resumes from their coverage.
His unsent edits keep their original reads and timestamps; age alone does not refuse them.

### Deletion

- After the grace period, choose announced, applicable snapshots covering the slots
  to delete. The usual grace is seven days from their latest announcement.
- Grace limits reload traffic; it is not a correctness condition.
  Deletion never waits for an absent device or an acknowledgement.
- First journal the new floor and deletion work. Conditionally write the floor
  with the snapshots and new `first` together; then delete slots below it.
- A conflicting floor write changes nothing. Read the current floor, reconsider
  the deletion, and never move `first` or any audience's coverage backward.
- A lost floor-write reply is settled by reading the floor before deleting.
  Delete only slots the confirmed floor puts below `first`.
- Failed deletions remain journaled and reported. No second floor update waits
  for deletion to finish.
- Every audience in deleted writes needs coverage. Missing or unverified coverage
  waits, including for deleted circles whose losses or source evidence remain needed.
- Any device with provider permission may delete covered slots.
  No deletion waits for the device that wrote them.
- Keep snapshots used by the floor, a pending floor write, or a still-needed raise or reset.
  Delete a superseded snapshot only after its applicable replacement passes grace and preserves the needed evidence.
- An audience without a member able to supply missing coverage has no deletion deadline.
  Already covered history does not wait for that member.
- Snapshots retain deleted-row generations and source verdicts.
  Old edits cannot revive a deleted incarnation or turn refused data into an accepted source.

Starting with genuine kept slots, validly announced honest floor snapshots and
retained raise/reset bodies, the modeled storage steps preserve those properties and
every sound reader ([`store_good`, `store_reader`](proofs/sequence/CovenSequence/Retention.lean)).
Under any interleaving of those steps and device steps, a running device with
fixed followed audiences and all their keys can still reach the current head's
full state ([`deletion_keeps_reachable`](proofs/sequence/CovenSequence/Device.lean)).
These results load ordinary snapshots from the floor and prepared bodies at
their matching raise/reset entries. Loading a prepared body directly from the
floor is not modeled.

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
- File status reads the fixed path's metadata, then checks the uploader's current state
  and any source failure known on this device (E8):
  - **Available:** storage contains the complete file at its fixed path.
  - **Integrity error:** storage holds an object at the fixed path whose
    size is not the file's complete encrypted size. That is damage, never
    Available and never absence
    ([`available_iff`, `wrong_size_is_damage`](proofs/storage/CovenStorage/Files.lean)).
  - **Missing:** storage confirms absence and the uploader was removed or
    replaced. This device also reports Missing for its own failed source.
  - **Uploading on D:** storage confirms absence and D is active, with no
    source failure known here. D may be offline or paused; bytes need not be moving.
  - Other devices cannot see D's source failure. D reports it to its own app.
  - A network or permission failure is an error, never proof of absence.
    With no storage configured, the call returns `NoStorage`.
  - Storage presence wins over a local source failure.
    No source failure is evidence that stored bytes are absent.
- A read can still use a checked local source or cached bytes. If it needs
  absent remote bytes, its typed error distinguishes uploading from missing
  and names the uploader, with the verdict file status gives
  ([`read_absence_matches_status`](proofs/storage/CovenStorage/Files.lean)).
  The app can ask for a missing file to be attached again.
- A ranged read still describes the complete object (§4), so a read checks
  the object's size as status does: any size but the file's complete
  encrypted size is an integrity error, even when the requested range is
  there. Every range a read requests lies within that size, because the
  header is checked against the row first, so a range the provider refuses
  as reaching past the object's end is the same damage, never absence. Status
  and reads agree on every storage read
  ([`read_agrees_with_status`, `wrong_size_read_is_damage`, `past_end_is_damage`, `present_never_absent`, `complete_object_reads`](proofs/storage/CovenStorage/Files.lean)).
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
    provider's `Storage` category and use §16.5's failure classes and retries;
  - confirmed absence with an active uploader and no local source failure is
    `Waits(Object(path))`; removal, replacement or a source failure on this device
    is `FileUnavailable(FileMissingReason)`. Removal or replacement requires app
    action; a source failure on its uploader retries under §16.5;
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
  the app declares with each namespace.
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
  - A file still queued here is pinned at once. The transaction retiring its
    upload carries the pin to the stored file, which coven then keeps whole
    from storage like any pin.
- The app can remove an uploaded file from the cache, which never touches
  storage.
- Eager fill keeps its requested file references until each settles, failures included.
  Evicting a completed fill does not request it again; a changed reference can.
- Cancellation commits before it is reported. Failures remain in `pending()`.
- Pin and eager-fill progress count authenticated bytes committed to the cache.
  Their one-shot reads and subscriptions report the same data (E4).
- Ana restarts after the provider confirmed two upload parts and the cache
  committed three download ranges. Her first reads and subscriptions show
  those same parts and ranges, even before storage reconnects. If open
  replaced her old device, both reset-notice reads return that stored notice.

### 16.5 Uploads and deletion

- Member-key replacement does not close a file folder. The slot cut-off
  decides write admission (§10); retained per-change losses keep their file
  references. File cleanup still requires snapshot/reference evidence and,
  for an orphan scan, confirmed provider cut-off.
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
  - Deleting or retargeting the row while its file uploads keeps the
    upload queued; it settles by its own reference, and only a confirmed
    upload retires it. Completion writes no row, so a deleted row stays
    deleted
    ([`steps_keep_upload`, `inflight_upload_settles`](proofs/storage/CovenStorage/Files.lean)).
- `_coven_user_files` keeps an original's path while a current row or a
  provisional local write can still name it. Applied deletion releases it
  in that same transaction when no row can return to it. A queued upload
  owns its captured source facts independently and keeps them until settled.
- Every attempt, including retries and resumed provider sessions, checks
  the source's size and whole-file content hash, and a user-provided
  original's recorded modification time, before sending file bytes.
  As it streams, it checks each plaintext chunk against its recorded hash
  before encrypting or sending that chunk. A missing or changed source
  fails the upload with the existing file error and records
  `FileUnavailable` for this device and file (§19.1).
  The local app sees the cause; a differing chunk is never encrypted or sent.
  - A retry must encrypt the same plaintext under the same key and chunk
    nonces ([§11.1](#111-cryptography)). The per-chunk check guarantees
    this even if the source changes after the whole-file check, without
    keeping a copy. Size and modification time alone cannot guarantee it.
  - A provider part may begin or end inside an encrypted chunk. Coven
    reads and verifies the whole plaintext chunk before encrypting it
    and selecting the requested bytes.
- Uploads retry missing, changed or mismatching sources and disk failures under
  backoff, checking the source before any provider request. Uploads and eager
  downloads retry storage failures by E5's classes: `Automatic` under backoff,
  `AppAction` on a credential or connection change without a timer, and `Never`
  without retrying, retaining the pending reason. `retry_uploads_now` optionally
  tries recoverable uploads early; progress never depends on it.
- Operations, invites, joining, missing keys and files share automatic delays
  of 1, 2, 4 … 256, then 300 seconds. Before waiting, persist wall-clock deadline T
  and full wait W with the work. Enforce the delay monotonically, unaffected by
  wall-clock changes.
  - Reopen arms `max(0, min(T - now, W))` monotonically, preserving T, W and the
    backoff exponent until another attempt.
  - Persist provider/account cooldowns separately at their actual scope before
    any affected worker retries. `Retry-After` can exceed five minutes; restart,
    sync_now and app retry never discard it.
  - Ana restarts with four seconds left of eight and waits four. A backward
    clock jump permits at most eight; a forward jump can reduce it to zero.
    This bounds restart waits, not real-time cooldowns under arbitrary jumps.
  - A failed delay commit fails its initiating work and prevents automatic retry.
- A large file goes up through the provider's resumable or multipart
  upload, in parts.
  - Providers require it above a size, such as Dropbox above 150 MiB per
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

- A stored file is deleted only when no retained row, loss, snapshot or
  queued or published write refers to its fixed path. Local rows, waiting writes, upload
  queues and journal-owned artifacts protect it too.
  - Read and authenticate all data needed to prove the file is unreferenced.
    Incomplete or unreadable data prevents deletion.
  - No idle pass lists files. File work follows changed references, uploads,
    history deletion or provider cut-off.
  - Under §13's storage cut-off assumption, the device responsible for a
    removed device's cleanup lists `<store>/files/<device>/` once after
    access is taken away. Cut-off must prevent in-flight sessions from
    publishing later too. This complete listing finds orphan files whose
    attaching rows did not survive; incomplete scans fail visibly and retry.
    A completed scan is not repeated after reopening.
    Restored access invalidates it; a later cut-off needs another complete scan
    ([`scan_names_every_file`, `retention_forgetting_counterexample`](proofs/io/CovenIO/Retired.lean)).
    Removal alone does not
    authorize a scan to be treated as the last one while storage access
    remains unresolved.
  - A device removed while its member keeps provider-account access has no
    cut-off and gets no scan: that account can still publish into its
    folder. Its orphans are found at the member's own later cut-off, whose
    scan lists the folder of every device the member held.
  - Ana removes Ben while his last photo upload is in flight. After his
    access is cut off, she lists his file folder: the photo either landed
    before cut-off and is included, or cannot land. Retained references and
    coverage still decide whether she can delete it.
  - Announcements and the floor name the snapshots to inspect.
    File retention does not list snapshot folders.
- Its storage path and fixed row reference carry the uploader's device
  id, so ownership remains known after the last reference disappears.
- If retained data belongs to an unreadable audience, lacks a key copy,
  or fails validation,
  a device cannot prove file absence and leaves uploaded files in storage.
  Retry only when its prerequisite or reader version changes, or the object
  is deleted. An unchanged pass does not download that data again.
- A device deletes uploaded files only with provider permission and the
  reference evidence above; a missing permission remains visible.
- Deleting a row deletes only coven's copies of its file, never a
  user-provided original.

### 16.6 What a device keeps about files

- Local file records describe sources, uploads, cache work and cleanup.
  They do not sync.
- A queued upload owns its fixed reference and source facts (§16.5).
  Retargeting its row cannot replace those facts or forget its unfinished upload.
- Ana attaches A, then changes the row to B before A finishes.
  Finishing A changes no row and cannot delete B's source or hashes.
- Native errors remain available during the process. After reopening, pending
  work exposes its persisted failure category (§19.1).
- Write and sync owned bytes to disk before committing a row that names them.
  Remove the row before deleting its bytes; a crash cannot leave it naming missing bytes.
- Record every new file's name before creating it. Attaching it clears its
  cleanup record in the same transaction; failure leaves recorded cleanup.
- An unfinished cache write keeps its own name until the transaction exposing
  its verified range commits. Opening never deletes bytes another live writer still owns.
- Cancelling staging or a cache write releases its active names.
  Durable records keep any abandoned bytes discoverable for deletion.
- A transaction releasing owned bytes records the deletion before committing.
  Delete those bytes after releasing the database writer, then remove the record.
- Run owed local deletions after writes and on opening.
  Failure retains the record. A committed write still returns its result; opening reports the cleanup failure.
- Original user files are never deleted by this work.
  File retention in storage follows [§16.5](#165-uploads-and-deletion).

## 17. Schema changes

### 17.1 Host application

- Every write records the schema version it was made with.
- Adding tables or columns is an *addition*.
  - Writes from devices on older versions still apply everywhere; they
    just don't set the new columns.
  - A device holds writes from a newer version until its app updates, then
    applies them.
  - E.g. Ana’s app adds `color`; Ben can still edit titles locally. Once
    he reaches a newer-version slot, sync waits for his app update before
    those queued titles can reach Ana.
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
  - Its statements decide this, whether or not a row matches on this
    device: a migration's class, and the conversions kept for it, never
    depend on a device's data
    ([`statement_plans_agree`, `row_classification_diverges`](proofs/queue/CovenQueue/ConversionChain.lean)).
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
    in an admin entry ([§9](#9-members-and-roles)).
  - An older app cannot apply that audience's newer schema. It records
    `UpdateRequired` and reloads from the snapshot after updating.
    Until updating, it follows [§6's stuck-device rule](#stuck-devices).
  - It snapshots every audience it can read, with one entry raising each
    to the new version; the store's own entry raises the store.
  - For a circle it can't read, the first device of one of that circle's
    members to update writes the circle's snapshot in the new version,
    with an entry raising the circle to it; the circle's other devices
    reload from that one.
  - A device that has migrated queues ordinary new-version writes behind
    its migration marker and sends them only after an applied raise
    to their version, its own or another device's, or once a later local
    breaking migration captures them (below).
  - A device whose app is older than the store stops uploading; after
    updating, it sends its waiting old-schema records unchanged, including
    records never attempted before migration.
  - A raise must increase the current version. An operation that finds its
    requested version already established adopts the announced state instead
    of claiming another raise. A higher raise validates against that state.
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
  - Its record in a writes slot names it and holds no row changes: its
    changes reach other devices only in the breaking change's snapshot.
  - So a device that runs the same migration later, then reloads from that
    snapshot, changes nothing anywhere with its own migration write; nor
    does one that adopts the already claimed raise.
- An update that runs several breaking migrations makes one migration
  write, and raises the store once, to the newest version.
- An update that only adds, run while a raise this device captured still
  waits to publish, moves that raise to its version: each waiting capture is
  taken again at the new schema through the device's newest write, with no
  new marker. A capture is always at the version its raise publishes, so the
  snapshot's header names it.
  - E.g. Ana migrates to 2 offline, then her app updates to 3, adding
    `color`. Her raise publishes version 3; Ben's app updates to 3 to load it.
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

- Dropping a table or column does not dismiss its pending losses.
  - Freeze each affected loss as old-shape data before removing its schema
    records. A whole-row loss keeps the whole captured row.
  - Keep its table and column names, key, audience, scalar values, setters
    and original cause. Strip live parent links; the old schema need not exist.
  - Frozen records no longer take part in merge or current removal rules.
    They remain readable, dismissible and included in snapshots.
  - E.g. Ana's migration drops `notes.color` while Ben's losing value “blue”
    is still pending. Every device keeps that loss under `notes.color`;
    opening the new schema does not silently discard it.
- Migration preserves all queued original records, tried or untried:
  device, number, timestamp, authored read-through slot, schema, disposition and
  plaintext remain unchanged; a tried record also keeps its format and
  sealing keys
  ([`conversions_preserve_uploads`, `conversions_preserve_attempt`, `conversions_preserve_untried`](proofs/queue/CovenQueue/Conversion.lean)).
- Publish those old-schema originals in write-number order, even when the
  local app is ahead of the store, under the ordinary identity and key gates.
  A migration appends one empty marker after the existing queue. The pass's
  writes slot can contain that eligible prefix and marker; later ordinary
  writes wait for their schema's raise.
- Confirm the captured originals and marker before uploading a raise snapshot.
  The snapshot excludes unclaimed app writes and includes migration effects
  over its covered prefix. Announce it, then claim the raise. Each contention
  reads and applies intervening slots; refresh the capture if its coverage
  is behind an earlier announcement. No empty queue is required (§15).
  In the queue model, capture freezes the prefix through the marker; later
  writes and their eligibility do not change that prefix's admission
  ([`captured_prefix`, `later_writes_do_not_delay`](proofs/sequence/CovenSequence/RaiseQueue.lean)).
  Given successful confirmation, upload, announcement and activation, the
  captured prefix is published once, the later queue stays waiting until
  activation, and all original fields and order are preserved
  ([`confirm_captured`, `confirmation_not_repeated`, `tail_waits`, `raise_without_waiting`, `originals_preserved`](proofs/sequence/CovenSequence/RaiseQueue.lean)).
  Upload requires confirmation, and activation requires the announcement
  ([`preparation_waits_for_confirmation`, `activation_waits_for_announcement`](proofs/sequence/CovenSequence/RaiseQueue.lean)).
  These results impose no storage-service or contention deadline.
- A later local breaking migration can include writes between its marker and
  the preceding marker in its new capture. Those writes and both markers
  publish before that raise; writes after the latest marker wait. Reopening
  retains the same queued originals, captured version and schema wait.
  Ana queues write 1, migrates with marker 2, writes 3, migrates with marker 4,
  then writes 5: 1–4 can claim one slot, the raise follows, and 5 waits for it.
  The queue model captures both markers and the intervening originals
  ([`recapture_includes_waiting`](proofs/sequence/CovenSequence/RaiseQueue.lean)).
- Before claiming its raise, a migrated device interprets uncovered older-schema
  changes using its local capture's conversion, preserving original records.
  Recapture includes every write already judged. If another device claims
  the version first, load that announced state and interpret uncovered writes
  under the same rule as every receiver; local migration results do not
  become shared losses.
  For an admitted next-slot part unchanged by the reset filter, queue
  interpretation equals sequence receipt using the original schema and applied version
  ([`late_original_converted`, `reload_matches_receiver`](proofs/sequence/CovenSequence/RaiseQueue.lean)).

- Conversion is for received late old-schema writes from other devices,
  including already-attempted writes that arrive after the raise. After adopting an
  already established raise, the original write's author uses that same receive
  rule to rebuild local state from the unchanged records
  ([`reload_matches_receiver`, `conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
  - Let S be the applied breaking-change snapshot for the audience: S supplies
    covered effects, with consumed slots and retained source verdicts
    kept separately (§8). Eligible uncovered ordinary old-schema changes
    use the applied raise's supplied conversions in version and causal
    order, independently of whether their upload was tried
    ([`receive_matches_evaluation`, `receive_agreement`](proofs/queue/CovenQueue/Conversion.lean)).
  - Those are the converters of the breaking migrations after the write's
    version through the raise's, in version order
    ([`one_step_is_conversion`](proofs/queue/CovenQueue/ConversionChain.lean)).
    If that range has no breaking migration, no converter is needed: eligible
    changes apply unchanged. Thus a write at the newest breaking version at
    or below the raise still applies when later versions only add tables or
    columns; older writes still need the conversions they cross
    ([`newest_breaking_write_admitted`, `retained_step_required`](proofs/queue/CovenQueue/ConversionChain.lean)).
  - Member-key slot cut-offs supply base verdicts to the same §8 data
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
  - If any breaking migration in that range lacks a converter, exclude
    the old-version interpretation: retain
    each original change as a frozen loss naming the breaking version,
    including a delete with no scalar values, and consume the write.
    Already frozen changes keep their original values and cause through
    later raises; no callback rewrites any tried or untried upload
    ([`missing_converter_preserves_values`, `conversions_preserve_frozen`, `lost_never_reconverted`](proofs/queue/CovenQueue/Conversion.lean)).
  - Recorded reads, including implicit own predecessors, govern receive
    order, not exclusion. No adoption-exclusion list is needed. Keep the
    selected raise/reset, snapshot coverage and `read_through` for their
    schema/reset admission checks, and retain per-change source verdicts independently
    of dismissible losses. A later snapshot covering an excluded position
    does not turn its dropped data into a source
    ([`recorded_reads_do_not_exclude`](proofs/queue/CovenQueue/Model.lean),
    [`snapshot_drop_stays_dropped`, `snapshot_loss_preserves_cause`](proofs/queue/CovenQueue/Verdicts.lean)).
  - Reset-ignored parts follow §19.3 before schema exclusion; they and
    superseded local migration markers create no schema loss. Otherwise eligible
    changes built on a reset-ignored source follow §8's data rule.
  - Commit interpreted changes, frozen originals, source verdicts, merged
    rows and consumed slots together. A failed conversion or reload
    preserves the preceding committed state and reports the failure.
- E.g. Ana’s announced snapshot covers a slot containing Ben’s write 8. His already-attempted
  old-schema write 9 arrives after the raise and creates a note. With the
  supplied converter, every device converts it on receipt, including Ben
  after adopting that raise; the upload's original bytes stay unchanged.
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
    announced state; reading the consumed identities of 9 and 10 alone does not
    exclude it.
- Loading the established snapshot replaces the local values another migration
  computed, which become no shared losses; ordinary excluded writes keep
  their original-value losses, judged alike by every receiver.
- When two devices prepare the same raise, the first valid claim establishes
  it. The other reads that slot, adopts its snapshot and undoes its unused
  artifacts. An announced preparation never activated by an admin slot is
  still its operation’s artifact: delete its body after validation fails;
  the announcement remains history and cannot be used for coverage.
  It publishes no losing entry. Reset/raise actions likewise
  validate against the actual preceding state, with §15's coverage check.

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
- What coven writes to storage has a *format*: sequence slots, inner writes,
  snapshots and paths, every byte given in
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
- A slot or inner write already tried is retried in the format of
  its first attempt, so re-sealing it reproduces the same bytes under the
  same content-bound nonces ([§6](#6-syncing-writes)).
- An older coven that encounters an object in a newer format asks for an
  update and waits, as with schema additions. Once updated, it reads that
  object and continues.
- Format versions belong to objects. A format change needs no administrative
  entry, snapshot or reload.

## 18. Operations

Every admin action uses one operation and its journal.
Role changes and renames use the same path as key rotation, invitations and resets.

1. Read and apply the sequence to its current end. Validate the requested action.
   A new call without storage fails before creating anything.
2. Journal each prerequisite before creating it: keys, grants, deliveries,
   snapshots or upload sessions. Keep enough information to finish or undo that work.
3. Do the prerequisite work, then claim the next slot under [§6](#settling-a-claim).
   Copies and snapshot announcements claim their own earlier slots.
4. After contention, apply the new slots and validate again.
   If still allowed, finish changed prerequisites and claim again.
5. If no longer allowed, undo the work the journal still owns and fail with
   `SyncError::Rejected(OperationRejection)`. Report an undo failure too and keep the journal for retry.
6. After the claim settles, apply the entry and finish its local work.
   Complete required custody before returning success.

With validation depending only on the preceding slots, the operation model
preserves recorded artifacts and in-flight requests through every step
([`steps_inv`, `step_req`](proofs/sequence/CovenSequence/Operation.lean)).
Starting with none of the operation's bytes in the sequence, it occupies at
most one slot; a completed claim passed validation against its filled prefix;
confirmed undo leaves neither its slot bytes nor its artifacts
([`operation_outcome`](proofs/sequence/CovenSequence/Operation.lean)).
This model retains sequence slots and proves safety, not eventual completion;
[§6](#settling-a-claim) gives the separate settlement proof after deletion.

- Contention does not return a decision to the app.
  An already-satisfied request completes without a redundant entry.
- Revalidation keeps the requested target and approved keys fixed.
  It cannot overwrite a competing member-key replacement or approve a different invite request.
- Before undo, await or cancel every artifact request already sent.
  Delete by the recorded name and confirm absence; a late result cannot escape the journal.
  Undoing before a create ends can leave an artifact after confirmed undo
  ([`eager_undo_leaks`](proofs/sequence/CovenSequence/Operation.lean)).
- A provider call that cannot be cancelled retains its cleanup obligation.
  S3 keys also carry the minting device's name for [§13](#13-removing-members-and-devices)'s reclaim rule.
- Deleting an absent artifact succeeds. A failed undo keeps both causes and
  retries; cancellation cannot report success while cleanup remains owed.
- Claimed prerequisite copies remain history. Undo cannot erase a disclosed key.
  Their exposure still affects key selection ([§11](#11-keys)).
- A claimed admin entry is final. After a crash, finish its required activation
  and custody; do not substitute another action.
- Local changes and journal progress commit together.
  Delete the unfinished-work record only after completion or confirmed undo.
- Dropping a call's future preserves its journal. Temporary failures and failed
  undo resume under [§16.5](#165-uploads-and-deletion)'s backoff.
- Each wait records its first unmet condition locally ([§19.1](#191-noticing)).
  The app may request a retry, but cannot discard required reloads or provider work.
- Claim progress requires successful IO and eventual scheduling.
  Contention has no fairness or elapsed-time bound.

### 18.1 Operations

- Rotation and circle creation keep the new key, claim its required copies,
  then claim its introduction. Adding a circle member first supplies historical keys (§11).
- An invite records its fixed target and new access before showing the code.
  Approval and undo follow [§12.2](#122-adding-a-person).
- Self-replacement pauses app writes, confirms its queue, keeps the new pair,
  then claims copies and replacement. Success installs custody and refreshes the restore code.
- A failed self-replacement releases the pause only after undo and confirmation
  that held credentials still match the current binding.
- A schema raise confirms captured writes and its marker, uploads and announces
  the snapshot, then claims the raise ([§17.1](#171-host-application)).
- Snapshot creation and retention follow [§15](#15-snapshots).
  Reload stages the required data before committing it atomically.
- Writes and files keep their journals with their queues.
  Provider-confirmed upload progress survives reopening; sending bytes alone does not advance it.

### 18.2 Example

- Carol's rotation journal holds K2 and its copies at slots 41 and 42.
  She records her attempt at 43, sends it, and crashes before receiving the reply.
- On reopening, she settles 43 first. If her rotation landed, she completes it.
  If another entry took 43, she applies it and validates again.
- If 43 removes Carol, her call undoes its remaining artifacts and fails.
  Copies at 41 and 42 remain exposure; undo cannot take them back.

## 19. Recovery

- Recovery reloads a usable installation, replaces a damaged one, or resets
  an audience from a device the person chooses.
- Coven reports failed input checks and blocked work to the local app.

### 19.1 Noticing

- `_coven_pending` holds one local record per subject.
  `pending()` and `subscribe_pending()` expose it to this device's app, including offline (E5).
- Each record names the first unmet condition. Keep it until that condition
  changes or the work settles; elapsed time alone cannot clear it.
- A blocked slot has one record naming its cause ([§6](#6-syncing-writes)).
  Do not add waits for every inaccessible later slot.
- Other subjects are writes, key copies, snapshots, files, operations, retention
  paths, audiences, joins and the connection. Key copies name the exact recipient credential.
- Refused cut-off writes keep their `WritesRefused` record (§10).
  A later slot cannot reverse that verdict.
- A permanent refusal requires a completely downloaded immutable object and a
  failed input check. A transport failure remains a failed read.
- Calls and pending records use the same `Refusal` variant.
  Calls retain native errors; durable records retain the category.
- An applicable coven update permits one recheck of a refused object.
  Reopening the same version does not repeat that work.
- A refused slot follows [§6's stuck-device rule](#stuck-devices); a later
  floor, reset or admin entry cannot bypass the refusal.
- Missing keys, required updates, failed reloads, unavailable files and failed
  cleanup stay visible. A failed snapshot read cannot justify loading an older state (§15).
- Applying a changed reset clears pending records with its reload.
  Failed reloads clear nothing. Journals and source evidence remain; unresolved work still reports its cause.
- A database transaction failure preserves the old state and applied slot.
  Storage corruption reports `Failed(LocalFailure::Database)` with the native cause on the call.
- Failure to save pending state stops sync and database calls with `DbError::PendingPersistence`.
  The app must not see an empty list after a failed save.

Ana's laptop needs an update at slot 80. Its app shows that cause and local data.
It cannot read Ben's removal at 81 or send queued writes until 80 applies.

### 19.2 Recovering one device

- At the app's request, a usable database reloads from snapshots and the
  sequence (§15), subject to [§6's stuck-device rule](#stuck-devices).
  It keeps its device id and waiting writes.
  - Settle attempted writes under [§6](#settling-a-claim). Convert
    eligible received old-schema records or record their original-value
    losses under §17.1, leaving upload records unchanged
    ([`reload_matches_receiver`, `conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
  - Reset boundaries still decide which parts apply (§19.3).
- A damaged database fails to open with `DbError::DamagedDatabase`.
  The app calls `CovenBuilder::reset_device` (E1).
  - Use §10's reset to a new device, from saved settings
    and custody; only §10's owed provider cleanup carries over.
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
    with the old and new ids (E5). Both notice queries
    report that unsent edits may have been lost, including after reopening.
    Failure to store the notice fails replacement; it cannot publish a usable
    handle that conceals the discarded installation.
- E.g. Ana's laptop has uploaded through write 12, but its damaged database
  may contain write 13. Resetting loads the stored history through 12 under
  a new device id. Coven does not guess which fragments of 13 are usable;
  the app receives the loss notice.

### 19.3 Resetting a store

- The person chooses the device whose data should replace the audience.
- For example, Ana asks to reset the store from her phone.
- An admin's device resets the store's rows; a member of a circle resets
  that circle's rows, one reset per audience.
  Store-reset authority is checked in the preceding slot state
  ([`store_reset_authorized`](proofs/sequence/CovenSequence/Authority.lean));
  the model leaves circle-reset authority to its supplied audience policy.
- A reset is an operation ([§18](#18-operations)):
  1. write a snapshot of what this device has ([§15](#15-snapshots));
  2. claim its snapshot announcement;
  3. validate again and claim the reset naming that announcement (§9).
- Every device reloads from that snapshot when it applies the entry, the
  resetting device too, so writes it applied after writing the snapshot
  are judged like everyone else's.
- Every later reset or schema raise takes effect in slot order and reloads
  its audience. The preceding entry remains part of history; it never drops.

- A reset replaces that audience with the chosen snapshot.
  The model's applied reset loads its chosen rows without replaying the
  pre-reset suffix ([`reset_replaces`](proofs/sequence/CovenSequence/Preparations.lean)).
  On every device, including its author, only these contents count:
  - the snapshot's rows and merge records;
  - later writes whose `read_through` includes the reset slot.
- A write outside the snapshot that had not read that entry is ignored
  for the reset audience, even if it read every write in the snapshot.
  - Do not apply its row changes or record them as lost.
  - Consume the ignored part so the slot can advance.
  - Other audiences' parts follow their own reset and schema rules.
  - Reading an ignored pre-reset write does not invalidate a post-reset
    write: the reset deliberately discards that old history.
- A device still uploads its unsent pre-reset writes, keeping their
  identities, sources and attempted inner bytes. It ignores their
  effects in the reset audience itself, like every other device.
- E.g. Ana resets from her phone's snapshot, covering Ben's writes through 8.
  Ben's laptop made writes 9 and 10 without reading the reset; 10 read 9.
  - Ben uploads both. Neither changes the reset audience or creates a loss.
  - After reading the reset, Ben makes write 11. It applies on every device.
  - Ana's own writes made after capturing the snapshot but before reading
    her reset entry are ignored by the same rule.
- If Ana and Ben both request a reset, the first claim applies. The other
  operation reads it, validates again and captures the newly read state
  before claiming a later reset. No snapshot can lower announced coverage.

## 20. Crates and conventions

### 20.1 Crates

- Coven is a Cargo workspace of eight crates, each owning one part of
  this spec:

| Crate | Owns | Spec |
| --- | --- | --- |
| `coven-foundation` | The clock, the id source, atomic file writes, the store's directory and its lock | [§7.2](#72-timestamps), [§10](#10-device-identity), [E1](api.md#e1-opening) |
| `coven-crypto` | Ciphers, sealed boxes, derived keys, file keys, member keys and their custody | [§11.1](#111-cryptography) |
| `coven-merge` | Timestamps, the merged state, the removal rules and lost values, as functions with no I/O | [§7](#7-order), [§8](#8-merge), [§14](#14-audiences) |
| `coven-format` | The bytes in storage: sequence slots, inner writes, snapshots, file headers and chunks, encoded, decoded and checked, using merge's and crypto's types | [Appendix D](format.md) |
| `coven-database` | The SQLite connection, coven's internal tables, applying the merge's results, triggers, live queries, migrations | [§5](#5-local-database) |
| `coven-storage` | Each provider, and the operations coven needs from it, including upload sessions | [§4](#4-storage-providers-and-access) |
| `coven-sync` | The store sequence, members, circles, snapshots, files and the cache, operations, recovery | [§6](#6-syncing-writes), [§9](#9-members-and-roles), [§12](#12-joining-and-restore) to [§19](#19-recovery) |
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
  files on disk    coven-foundation's files
  ```

- Everything else reaches a capability through the object that owns it,
  given to it when it is built.
  - The builder configures the clock, custody and OAuth. Ids use OS randomness;
    only `test-utils` permits replacing their source ([E1](api.md#e1-opening)).
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
  set up. `start_sync` starts the loop, whose first pass builds the client
  from custody credentials (E5). Repeated starts keep the running loop.
- `stop_sync` returns immediately; release follows the active pass and transfers.
  It retains session keys, location and credentials. Status reports completion
  or `Failed` with its cause, without a pending connection record.
  `disconnect_storage` also removes credentials; remote contents stay untouched (E5).
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
  including maintenance and prerequisites
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
      merge records, losses, the applied slot and query notifications;
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

- Problem: SQLite-chosen keys (integer rowid aliases) can collide offline.
- Status: refused; content-derived integer keys are allowed
  ([§8.5](#85-keys-and-uniqueness)).

### A.2 Tables with no primary key

- Problem: coven can't tell which row a change belongs to, and SQLite's
  hidden rowid collides across devices like an integer key.
- Status: refused ([§8.5](#85-keys-and-uniqueness)).

### A.3 Unique constraints besides the primary key

- Problem: two offline devices can each insert a row with the same value.
- Status: the row with the first slot claim keeps the value;
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
  to the existing foreign-key action restrictions: a local key into a synced
  non-key column follows its updates with cascade or set null.

### A.10 Collations on keys

- Problem: a key or unique column with `COLLATE NOCASE` or `RTRIM` makes
  SQLite treat different spellings as one value, while each device's write
  records the spelling it used. Two devices adding "Urgent" and "urgent"
  would make two rows to coven where SQLite can hold one, and two claims of
  one unique value that coven sees as different.
- Status: refused on open and after migrating; key, unique and foreign-key
  columns of synced tables compare by bytes ([§8.5](#85-keys-and-uniqueness)).
