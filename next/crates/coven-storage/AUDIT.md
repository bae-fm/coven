# Storage implementation report

This report describes coven-storage against the revised [spec](../../../plans/coven-from-scratch.md).
Random file IDs, audience-specific snapshot paths and prefixes for all logs are
implemented. The other findings remain unresolved because the contract requires
stopping when a provider cannot supply the requested behavior; the sharing
limitation below triggers that condition.

## Audit table

“Implemented” below means the operation exists and the named tests exercise it;
it does not imply the stub proves the real provider's guarantees. “Incomplete”
identifies a missing capability, a contract violation, or a test that assumes
the provider behavior it should establish. All paths below are relative to
[`src`](src/).

The shared `test_utils::Conformance::run` exercises nonempty creation, a whole
read, three ranges, rejection beyond EOF, occupied-path refusal, same-byte
`create_once` retry, prefix filtering, repeated deletion, missing reads,
positions replacement, probe, and setup in an empty location. It does not
exercise multipart uploads, account permissions, concurrent setup, empty
objects, large ranges, or replacement of immutable paths.

### S3

Source: [s3.rs](src/providers/s3.rs).
Tests: [s3_tests.rs](src/providers/s3_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes (§6, §9, §11, §12.2, §15, §18) | Implemented: AWS SDK `PutObject` with `If-None-Match: *`; common `create_once` reads and compares an occupied object. | `real_s3_client_conforms_with_pagination`; no concurrent HTTP create race, empty-object test, or compatible-provider recordings. |
| Replace posted positions (§6) | Implemented: unconditional `PutObject`, only for `positions/<device>`. | Shared conformance checks two replacements. Immutable-path rejection is not exercised. |
| Read whole / range (§4, §16.3) | Implemented: `GetObject`, optional `Range`, checked `Content-Range` and response length. | Shared conformance; multipart test reads the final byte. No 1 MiB range or interrupted response-body test. |
| List prefix with paging (§4) | Implemented: `ListObjectsV2`, scoped prefix, continuation tokens, duplicate-path elimination, repeated-token refusal. | Conformance stub returns one object per page. Malformed/repeated tokens are not tested. |
| Delete / deletion eligibility (§15, §16.5, §18) | Implemented: `DeleteObject`; missing key succeeds. `HeadObject` is used before reporting `DeletionRights::Delete`; sync must choose the eligible device. | Repeated deletion in conformance. Existence does not establish the caller's delete permission; actual deletion can still fail. Bucket versioning behavior is not tested. |
| Recorded multipart upload (§16.5, §18) | Implemented: create upload; record ID, unique metadata token, size and part ETags; `UploadPart`; paged `ListParts`; conditional `CompleteMultipartUpload`; `HeadObject` verifies a lost completion against the session's token. | `multipart_recovers_lost_part_and_completion_replies`, `multipart_sizes_parts_for_the_promised_object`, `missing_session_and_destination_is_expired`. Part-list paging, conflicting completion, and abort retries are not exercised. |
| Grant / revoke access (§4, §12.2, §13) | Implemented manual result: `CreateAccessKey` / `DeleteAccessKey { access_key_id }`. No IAM call is made. | `errors_and_manual_key_instructions` checks the public key ID returned to the admin. |
| Member's key / device removal (§11, §20.9) | Implemented constructor takes explicit access ID and secret; SDK credential discovery is not used. Device sign-out returns `ReplaceAccessKey`. | Constructor is used by all S3 stub tests; sign-out is asserted. Stub does not validate SigV4, principal isolation or actual console policy. |
| OAuth / CloudKit calls | Not applicable. | No live account or console action. |
| Setup / probe / failures (§20.5) | Common setup and probe operate through the real SDK adapter. S3 error codes classify missing bucket, wrong region, credentials, permissions, quota, throttling and network failures. | Conformance, `setup_refuses_an_unrelated_object_in_the_location`, `errors_and_manual_key_instructions`. No simultaneous setup or cleanup-failure test. |

The conditional requests correspond to AWS's documented
[PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
and [multipart completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).
These documents establish AWS behavior, not every service accepting an S3 URL.

### Google Drive

Source: [google_drive.rs](src/providers/google_drive.rs).
Tests: [google_drive_tests.rs](src/providers/google_drive_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes | **Incomplete for the single-writer contract:** full coven path is a Drive filename in one folder. `files.list` checks absence; `files.generateIds` assigns a new ID; resumable `files.create` publishes; `ensure_unique` selects the lowest ID and deletes the caller's duplicate. | Conformance; `concurrent_create_once_keeps_the_same_file_on_both_devices`; `two_uploads_cannot_publish_different_bytes_at_one_path`. Those tests exercise cross-device races that §4 no longer requires. No test checks recovery of this device's duplicates by creation time. |
| Replace posted positions | Implemented for a found ID: media `PATCH`; absent name goes through creation. | Conformance. Retries after a lost first-creation reply and preexisting duplicate filenames are untested. |
| Read whole / range | Implemented: resolve filename with `files.list`, then `files.get?alt=media`, with `Range` for partial reads. | Conformance; end-of-upload range check. Native provider redirects, partial body failures and 1 MiB requests are untested. |
| List prefix with paging | Implemented: list the folder's untrashed children across `nextPageToken`; parse full filenames; filter prefix locally. | One-entry pages in conformance. The implementation discards duplicate filenames and does not return their storage times. |
| Delete / uploader restrictions | Implemented: `ownedByMe && canDelete` permits permanent deletion; otherwise `canRemoveMyDriveParent` permits `files.update?removeParents=...`. Public `covenDevice` metadata supplies the uploader device. | `session_reopens_and_only_uploader_deletes` verifies unlinking another uploader's file without deleting its bytes. Shared-drive ownership, neither permission, missing metadata and duplicate filenames are untested. |
| Recorded resumable upload | Implemented: generated file ID and upload URL are recorded; chunk `PUT`s use `Content-Range`; empty `PUT` queries progress; a lost final reply is checked by file ID/name/size/parent. | `session_reopens_and_only_uploader_deletes`, `interrupted_part_continues_at_the_confirmed_byte`, `missing_session_and_destination_is_expired`. Selecting the lowest generated ID does not implement keeping the earliest stored copy and removing all of this writer's later copies. Abort's DELETE/499 handling has no test or Drive-specific documentation established by this audit. |
| Share an account | Implemented: paged folder `permissions.list`, create or upgrade a user permission to `writer`, then verify. | `conformance_and_account_sharing` covers a new grant. Existing reader upgrades, inherited grants, account aliases and permission paging are untested. |
| Take back access | Implemented for matching visible user permissions: `permissions.delete`, then list again. | Same test covers one direct grant. Inherited access, groups and duplicate/overlapping invites are untested; the current spec prohibits removing the store owner. |
| OAuth / device removal | Shared OAuth clients implement Google consent, code exchange and refresh. Sign-out instructs the member to remove the app's provider access. | Shared OAuth tests below; no provider-owner method installs refreshed tokens on an already constructed adapter. |
| Setup / probe / failures | Common implementation operates on the supplied existing folder ID. It does not create a folder or check folder identity/ownership/sharing capabilities. | Conformance only for valid writable folder. Shared HTTP tests classify a subset of Google errors. Missing container and missing object are not distinguished at the HTTP adapter. |

Google documents that a filename need not be unique within its folder in the
[File resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/files).
[Generated IDs](https://developers.google.com/workspace/drive/api/guides/create-file)
prevent duplicate creation when retrying **the same ID**. The single-writer
contract allows recovery of a writer's own duplicate files; the adapter still
orders by ID rather than creation time and does not remove every later copy. The
[upload guide](https://developers.google.com/workspace/drive/api/guides/manage-uploads)
documents resumable status queries and chunk alignment. Its 5 MB distinction
does not mean each resumable request is capped at 5 MB; the adapter's 8 MiB
chunks are not, by themselves, a defect.

### Dropbox

Source: [dropbox.rs](src/providers/dropbox.rs).
Tests: [dropbox_tests.rs](src/providers/dropbox_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes | Implemented: `files/upload`, `mode=add`, `autorename=false`, `strict_conflict=true`; namespace fixed by `Dropbox-API-Path-Root`. | `conformance_ranges_pagination_and_account_sharing`. No empty-object, concurrent-conflict or request-size boundary test. |
| Replace posted positions | Implemented: same endpoint with `mode=overwrite`, restricted to positions. | Conformance. The adapter does not select an upload session when this body exceeds the endpoint limit. |
| Read whole / range | Implemented: `files/download`, optional HTTP `Range`, common status/range/length validation. | Conformance and `range_reads` assertion; final-byte multipart check. No 1 MiB or interrupted-body coverage. |
| List prefix with paging | Implemented: recursive `files/list_folder` for the namespace, `list_folder/continue`, local prefix filtering, cursor loop detection. | One-entry pages in conformance. Folder entries and invalid/repeated cursors are untested. |
| Delete / deletion eligibility | Implemented: `files/delete_v2`; classified missing path succeeds; `get_metadata` precedes `DeletionRights::Delete`. | Conformance. The metadata read does not establish delete permission. |
| Recorded upload session | Implemented: start, append at byte offset, finish with strict create conflict; record session ID; empty append queries offset; closed/absent session plus existing destination triggers byte-by-byte verification through range reads. | `resume_queries_the_stored_offset_after_lost_part_reply`, `lost_completion_requires_byte_verification`, `missing_session_and_destination_is_expired`. Abort closes the session but has no retry/status test. |
| Share an account | Implemented: `sharing/list_folder_members` plus continuation; add editor; verify. Existing non-editor is removed before re-adding. | Conformance covers an existing Dropbox user as editor. Pending invitations, viewer upgrade failure, folder owner and ACL policy are untested. Removing the previous grant before a failed add is not atomic. |
| Take back access | Implemented: `remove_folder_member`, `leave_a_copy=false`, wait for asynchronous job, verify absence. | Stub returns immediate completion only. Job progress/failure/timeout, permission paging and owner refusal are untested. A job's typed failure payload is discarded. |
| OAuth / device removal | Shared OAuth clients implement offline tokens, code exchange and refresh. Sign-out instructs removal of app access. | Shared OAuth tests; same installed-token ownership gap as Drive. |
| Setup / probe / failures | Common implementation assumes an existing shared namespace. No folder creation, sharing-policy setup or recipient mount operation exists. | Conformance verifies the header, but the fake gives every token immediate access to the same namespace. |

Dropbox's official [files API schema](https://github.com/dropbox/dropbox-api-spec/blob/main/files.stone)
defines strict conflicts, the 150 MiB single-request limit, and session expiry.
Its [sharing schema](https://github.com/dropbox/dropbox-api-spec/blob/main/sharing.stone)
defines account membership and recipient mounting. The spec limits membership
changes to the store owner; the adapter does not establish that authority or
mount the folder for a joining recipient. Closing an upload
session is also not a documented immediate deletion of its buffered bytes.

### OneDrive

Source: [onedrive.rs](src/providers/onedrive.rs).
Tests: [onedrive_tests.rs](src/providers/onedrive_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes | Implemented for nonempty data: create parent folders with conflict failure, `createUploadSession` with conflict behavior `fail`, then chunk PUTs. Empty data uses content PUT with conflict/conditional headers. | `conformance_hierarchy_pagination_and_sharing` tests nonempty conflicts. Stub's ordinary PUT overwrites unconditionally, so the separate empty-object path is not verified. |
| Replace posted positions | Implemented: parent creation and content PUT, restricted to positions. | Conformance. No atomicity/race or endpoint-size test. |
| Read whole / range | Implemented: get item metadata, then GET the preauthenticated download URL without a bearer token; range validated by common HTTP code. | Conformance; `resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls`. Redirects, expired download URLs and interrupted bodies are untested. |
| List prefix with paging | Implemented: walk child folders, follow `@odata.nextLink`, validate same origin, refuse cycles, filter parsed paths locally. | One-entry pages in conformance. Hostile/repeated links, folder cycles and malformed item types are untested. |
| Delete / deletion eligibility | Implemented: item DELETE by path; HTTP 404 succeeds. Metadata lookup precedes `DeletionRights::Delete`. | Conformance. Missing drive versus missing object and actual delete permission are not distinguished. |
| Recorded upload session | Implemented: store transfer URL, GET status without bearer, parse `nextExpectedRanges`, resume aligned parts; lost final reply falls back to destination size/name then full byte verification. | `resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls`, `lost_completion_requires_byte_verification`, `missing_session_and_destination_is_expired`. Only a single open-ended expected range is accepted; bounded/multiple ranges are untested and refused. |
| Share an account | Implemented: list permissions, POST `invite` with email/write/sign-in required, then verify. | Conformance supplies `invitation.email` and globally visible permissions. Actual recipient identities after acceptance, non-owner visibility, inherited grants and paging are untested. |
| Take back access | **Incomplete:** delete every email-matched permission returned by listing; no recorded invite grant identifies the intended permission. A matching multi-recipient permission is deleted in full. | Stub exposes all grants to every caller. The revised contract requires the owner's credentials; that precondition is not checked. No test asserts that a removed account subsequently cannot access the folder. |
| OAuth / device removal | Shared OAuth clients use the Microsoft `consumers` endpoint, delegated file access and offline refresh; device removal gives app-revocation instructions. | Shared exchange test; work/school sign-in is not supported by the selected authority, and this restriction is not represented in `CloudProvider::OneDrive`. |
| Setup / probe / failures | Common setup assumes the drive and root folder exist. It never establishes provider ownership or sharing authority. | Conformance only. Common HTTP tests exercise selected Microsoft error codes. |

Microsoft documents [upload sessions](https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0)
with 320 KiB alignment, automatic final publication, expiry, and bounded or
multiple missing ranges. It documents the folder-sharing endpoint as
[driveItem invite](https://learn.microsoft.com/en-us/graph/api/driveitem-invite?view=graph-rest-1.0).
Microsoft's [permission listing](https://learn.microsoft.com/en-us/graph/api/driveitem-list-permissions)
returns only permissions applying to a non-owner caller. Absence from that
response is not evidence that another account has no access. Owner-only sharing
resolves this provider restriction; it still needs to be represented at the
caller/provider boundary.

### CloudKit

Source: [cloudkit.rs](src/providers/cloudkit.rs).
Tests: [cloudkit_tests.rs](src/providers/cloudkit_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once / replace positions | Adapter delegates to app `CloudKitOps::create` / `replace`; positions-only guard is in Rust. Bridge contract requires stable record IDs and server-enforced creation/atomic replacement. | `bridge_conforms_and_retains_parts_across_adapter_restart` runs conformance against a memory bridge. No native save-policy behavior is tested. |
| Read whole / range | Adapter delegates to bridge `read`, checks ranged length. Bridge is required to fetch only covering asset parts. | Memory bridge slices a whole in-memory object; it proves neither CKAsset range efficiency nor bounded network transfers. |
| List prefix with paging | Bridge owns native cursor traversal; Rust validates prefix membership and duplicate paths after it returns. | Memory listing only. No cursor, incomplete enumeration, duplicate or wrong-prefix test. |
| Delete / deletion eligibility | Delegates deletion; reads the entire object before reporting `DeletionRights::Delete`. | Memory conformance only. Reading an object is not evidence of permission to delete it, and whole-file reads for this check are avoidable. |
| Recorded multipart upload | Delegates durable session start/status/part/finish/abort; Rust records opaque ID and maximum part size, checks offsets, advances only after reply. | Lost part reply and adapter recreation are tested while the same in-memory bridge survives. No native/app-process restart, durable parts, lost final reply, abort or publication atomicity test. |
| Share / revoke | Delegates `set_access(email, bool)`, intended to use `CKShare` participation. | No invocation of grant/revoke in the CloudKit test. Fake imposes no owner restriction or acceptance requirement. |
| Join with invite | **Missing bridge capability:** no returned share URL/metadata and no share-acceptance call. Location contains container/owner/zone only. | No recipient join or acceptance test. The bridge's read/write contract does not define this step. |
| OAuth / device removal | No OAuth: native Apple account is used. Sign-out returns `RemoveFromAppleAccount`. | Sign-out result is asserted; actual Apple account actions are outside Rust. |
| Setup / probe / errors | Common setup/probe delegate to supplied operations. Bridge must preserve native error causes and classifications. | Memory conformance and injected permission failure only. No zone creation, native error or Apple account availability test. |

Apple's [shared-records guide](https://developer.apple.com/documentation/cloudkit/shared-records)
requires share metadata and `CKAcceptSharesOperation` before a recipient can use
the shared zone. Its [participant documentation](https://developer.apple.com/documentation/cloudkit/ckshare/participant)
assigns private-share participant management to the owner, as the revised spec
requires. The native bridge must enforce that precondition.
[CKAsset](https://developer.apple.com/documentation/cloudkit/ckasset) is fetched
as an asset with its record; arbitrary byte-range HTTP reads are not a native
CKAsset operation established here. Section 20.1 already calls for bounded
assets, so range support needs a tested native representation of those parts,
not an assertion that every CKAsset supports Range.

## Requirements shared by the providers

| Requirement | Finding and evidence |
| --- | --- |
| Exact layout (§4, §6, §9, §11, §12.2, §15, §16.2) | `ObjectPath` models device logs, store logs, positions, store/circle snapshots, sealed keys, random `FileId` names and join requests. Parsing rejects superseded hash names and snapshots without an audience. `device_logs()` and `store_logs()` include every device. Three path tests cover the layouts, serialization, family boundaries and snapshot device extraction. |
| Encryption boundary (§11, §16.2, §21) | Storage accepts already encrypted bytes and paths built with foundation's `FileId`; it does not calculate or send a plaintext content hash. Encryption, authentication and chunk format belong to crypto/format and callers. Their end-to-end use was not audited here. |
| Neighbouring chunks, at most 1 MiB (§16.3) | Every adapter accepts an arbitrary contiguous `ByteRange`; none knows chunk/header boundaries, caches the header or combines chunk requests. Those decisions belong to sync's file reader. No storage test requests 1 MiB or verifies a request ceiling. Whole response buffering means chunks cannot be checked before the requested response is complete. |
| Storage residence time for deletion (§15) | **Missing:** `list` returns only paths; `read` returns only bytes; no public object metadata operation exposes upload/modification time. Sync cannot learn how long storage has held an object from these operations, especially after a restore. Author timestamps cannot establish upload time. Object length is also unavailable without a read. |
| Durable session recording (§16.5, §18) | `UploadSession::encode/decode` carries provider state, location, destination, total, confirmed offset and part size. Tests recreate adapters and deserialize old values. Actual database recording belongs to sync/database and was not checked. No test covers a crash after remote session creation but before its ID is recorded. |
| Setup atomicity (§20.5) | `Storage::setup` lists the location, then creates the first entry, or accepts an identical sole entry. It commits no local settings/credentials. `StorageSettings` uses foundation's atomic file replacement. Credential/key/settings commit and rollback are facade responsibilities, not implemented by this crate. |
| Setup occupancy (§20.5) | Two callers can both list an empty location and create first entries under different device paths, as revised §4 allows; sync detects the competing store later. The new `store_logs()` prefix exposes both entries. Existing setup tests cover sequential retry/occupation only. No simultaneous-setup test exists. Setup currently refuses any listed object or malformed path, which is stricter than checking only for another store. |
| Probe (§20.5) | Common probe checks create, whole read, range, conflict, list, delete; retains both check and cleanup failures. It does not probe positions replacement, multipart transfer, sharing authority, or recipient access. A lost create reply returns before cleanup, leaving an unrecorded probe object. Existing tests exercise only the successful path. |
| OAuth sign-in and refresh (§20.10) | `OAuthClients` builds provider-specific authorization requests with state and PKCE, exchanges codes, computes expiry with the injected clock, refreshes and retains an omitted refresh token. Browser flow binds localhost port 19284, validates the callback and responds to cancellation/deadline. Shared tests cover all three exchange providers, state/provider/redirect binding, injected time, Dropbox refresh failure/retention, network failure, local callback and cancellation. No real browser, provider registration, consent or mobile redirect was tested. |
| Installing refreshed tokens (§20.10, §21.2) | **Missing owner access:** `OAuthSession::set_tokens` exists, but each adapter consumes and privately owns its session, exposes no token-update method and does not share the session handle. A caller cannot update the constructed adapter through its capability. Reconstructing an adapter is a different operation; no test demonstrates committed refresh followed by an authenticated request on the same adapter. |
| Typed failures (§20.5, §21.3) | `StorageError`, `StorageFailure`, setup errors and OAuth errors preserve most native/HTTP causes. `ProviderResponse` and S3 error wrapper redact ordinary diagnostics. `http_tests` and `error_tests` cover selected mappings, redaction and cause retention. HTTP missing-container responses have no context-specific distinction, and Dropbox async removal converts the failure to a static protocol string, losing its original payload. |
| Member/device actions (§20.9–§20.10) | S3 manual actions and provider sign-out instructions are represented. Shared-folder grants are identified only by email, not by a recorded grant or invite. Multiple invites to one account and access predating an invite cannot be distinguished: cancellation can revoke access another invite/member still needs. No such ownership test exists. |
| Crate rules (§21) | Storage depends on foundation/crypto and external providers, not database. Production/test files are below 1,000 lines and tests are siblings. It offers memory storage, faults and conformance under `test-utils`. §20 describes the new path constructors and log prefixes. `FileId` is defined alongside the other UUID IDs in foundation; storage has no new crate dependency. |

The shared sources are [storage.rs](src/storage.rs),
[session.rs](src/session.rs),
[path.rs](src/path.rs),
[http.rs](src/providers/http.rs), and
[oauth.rs](src/providers/oauth.rs).

## Provider limitation that stops implementation

The required behavior is to record the grant each invite made, and revoke
exactly that grant without revoking another invite's access. Dropbox exposes
membership of a folder by an account, not a separately identified grant for
each invite.

Its official [sharing API schema](https://github.com/dropbox/dropbox-api-spec/blob/main/sharing.stone)
defines `RemoveFolderMemberArg` with only `shared_folder_id`, `member` and
`leave_a_copy`. `MemberSelector` is an email or Dropbox account/group ID.
`add_folder_member` returns no grant identifier; listed memberships likewise
have no grant version or incarnation. The schema offers no conditional removal
of a particular version of that membership. It was fetched again on
2026-10-06 and matched the schema examined in the original audit.

A concrete protocol counterexample:

1. An invite granted account X access to folder F. Its revocation succeeds at
   Dropbox, but the response is lost before coven records completion.
2. Another device of the owner re-invites X, granting access to F again.
3. The first device retries its recorded revocation. The provider request still
   names F and X, so it removes the later invite's access.

Recording the account's stable Dropbox ID instead of its email does not change
this result. A preflight membership read has the same missing grant identity
and cannot make the following removal conditional. An asynchronous job ID helps
only when coven received and recorded that ID; it cannot recover an initial
removal reply that never arrived.

This is a limitation inferred from the documented request and response types,
not a live-account reproduction. A rule governing overlapping invitations and
unconfirmed removals across the owner's devices is needed before the promised
isolation can be implemented. No serialization policy, reference-count protocol
or alternative sharing mechanism is selected here.

Google's [permission resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/permissions)
also describes its permission ID as identifying the grantee. Merely changing an
email field into a field called a grant ID is not evidence of distinct grants
across re-invites. The Dropbox counterexample alone establishes the stop.

The previous stops are resolved by the revised contract: paths have one writer,
sharing uses the owner's account and the owner cannot be removed, expired upload
sessions restart from retained bytes, and setup races are detected by sync after
setup. Their implementation gaps remain in the tables and list below.

## Additional confirmed defects and untested boundaries

These findings remain unimplemented because the provider limitation above
triggers the requested stop condition.

- **Drive does not resolve its own duplicates as specified.** It orders by
  generated ID instead of storage time and deletes only a losing current
  upload, leaving previously stored duplicate copies.
- **Expired sessions do not restart.** Each adapter reports `SessionExpired`;
  there is no storage operation or caller in this crate that begins a new
  recorded session and continues from the retained bytes.
- **Single-request limits are not available to callers.** `Storage` exposes
  multipart methods but no provider transfer limits. Dropbox `create` always
  uses one request, including a possible 1 GB write record (§6), above the
  documented 150 MiB limit. S3 uses one `PutObject`; all positions replacements
  use one request. Callers need a defined selection rule and boundary tests.
- **Multipart abort is not consistently retry-safe.** Dropbox's second close
  can return `closed`, and a lost part reply leaves the recorded offset behind
  the provider; `abort_upload` accepts neither case. Its test endpoint ignores
  `close` entirely, and no abort test detects it. Drive's cancellation behavior
  requires provider evidence. CloudKit abort has only the trait's promise.
- **OneDrive rejects documented resume responses.** `progress` requires exactly
  one `nextExpectedRanges` item ending in `-`. A valid bounded range or more
  than one missing interval becomes `Protocol`. No stub response covers either.
- **Session decoding validates only shared scalar bounds.** It does not check
  that provider/location and state variant agree, that S3 part counts/sizes and
  confirmed offset agree, or that provider IDs are nonempty. Provider methods
  check some of these later, inconsistently; `Complete` returns early in several
  methods. The sole direct session test exercises serialization capacity and
  escaping, not malformed recordings.
- **OneDrive/Dropbox completion verification can use inconsistent revisions.**
  `VerifyPublished` reads separate ranges by path with no fixed object version.
  An immutable file path mitigates ordinary writes, but `begin_upload` also
  accepts replaceable positions paths. Concurrent replacements can supply
  matching parts from different versions. The API neither restricts such
  sessions nor binds verification reads to a revision. No test exercises it.
- **Sharing mutations can affect grants beyond the invite.** Email-only
  matching cannot establish which permission the operation owns. OneDrive can
  match a permission containing several recipients, then delete the entire
  permission. Dropbox removes an existing viewer before trying to add an
  editor, so a failure can remove previously valid access. These flows have no
  rollback or ownership tests.
- **Deletion-rights results overstate what was checked.** S3 HEAD, Dropbox
  metadata, OneDrive metadata and CloudKit read establish readability, not
  deletability. Google checks actual provider capabilities, but its
  `ownedByMe` policy refuses shared-drive deletion even when `canDelete` is
  true. Shared-drive removal cannot use a My Drive parent capability. Supported
  folder kinds and the meaning of `DeletionRights` need explicit treatment.
- **Recipient onboarding is not exercised.** Tests share into an existing fake
  location; they never authenticate as the recipient and perform the first join
  request. CloudKit acceptance and Dropbox mounting are missing from the
  capability surface. Existing-folder creation/selection and supported account
  types are likewise not setup capabilities in this crate.
- **Error coverage does not establish all promised classifications.** There
  are no provider tests for rate-limit headers, permission failures on each
  operation, malformed success bodies, session URL changes, missing container
  versus missing object, rollback after sharing failure, or probe cleanup
  failures. OneDrive `nextExpectedRanges` failures and Dropbox removal-job
  failures need native causes retained.
- **The memory fake is not a complete model of the contract.** It reports the
  current offset without checking monotonic progress and keeps pending uploads
  after completion; its S3 configuration also accepts provider-account
  revocation. Real providers' session expiry, ownership and invitation
  acceptance are absent. Its successful CloudKit tests cannot validate these
  provider properties.

## Commits

- `236f3ecf` (`Record provider constraints before completing storage`): records
  the initial provider audit before implementation. Its original provider stops
  were resolved by the revised specification; the remaining findings are kept
  here.
- `ad888f0e` (`Keep uploaded copies and snapshot audiences distinct`): adds foundation's
  UUID `FileId`, uses it for `files/<id>`, gives store and circle snapshots
  separate constructors, adds prefixes for all device and store logs, updates
  every storage fixture and §20, and rejects the superseded path layouts.
- `Keep the unresolved storage contract visible`: moves the still-applicable
  audit findings into this report, records the sharing limitation, and deletes
  `plans/coven-storage-audit.md`.

These commits are local; no branch was pushed. No other implementation concern
was committed after discovering the remaining provider limitation.

## Verification and what was not checked

The full `next/scripts/check.sh` passed with
`export PATH="$HOME/.elan/bin:$PATH"`, without overriding `CARGO_TARGET_DIR`,
`RUSTC_WRAPPER` or `CARGO_INCREMENTAL`: formatting, clippy, ownership policy,
documentation, production compilation, both test feature configurations, both
Lean proofs and the Rust/Lean differential test. All 41 storage tests passed in
each feature configuration. The commit hooks also passed. Passing these tests
does not cover the gaps listed above.

No live provider account, native CloudKit implementation, console-created S3
policy, real OAuth browser/consent flow, mobile platform, or non-macOS execution
was checked. This audit did not review database/sync implementations, key
custody transactions, snapshot/file retention orchestration, or the caller's
chunk-combining/cache behavior. The sharing counterexample is a protocol finding,
not a newly executed reproduction. The path regression test was run before the implementation and
failed on the random file path, then passed after the change. Existing provider
tests now exercise UUID file paths; their HTTP behavior is otherwise unchanged.
