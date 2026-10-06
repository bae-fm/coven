# coven-storage audit

Audited on 2026-10-06 at `391ec925d6a338cdd6424d645eecc955ccb46171`.
`next-storage-audit` matched the fetched `origin/main` before this report.
The entire [spec](coven-from-scratch.md) and every tracked file in
`next/crates/coven-storage/` were read before writing this report. No production
or test code was changed for the audit.

## Audit table

“Implemented” below means the operation exists and the named tests exercise it;
it does not imply the stub proves the real provider's guarantees. “Incomplete”
identifies a missing capability, a contract violation, or a test that assumes
the provider behavior it should establish. All paths below are relative to
[`next/crates/coven-storage/src`](../next/crates/coven-storage/src/).

The shared `test_utils::Conformance::run` exercises nonempty creation, a whole
read, three ranges, rejection beyond EOF, occupied-path refusal, same-byte
`create_once` retry, prefix filtering, repeated deletion, missing reads,
positions replacement, probe, and setup in an empty location. It does not
exercise multipart uploads, account permissions, concurrent setup, empty
objects, large ranges, or replacement of immutable paths.

### S3

Source: [s3.rs](../next/crates/coven-storage/src/providers/s3.rs).
Tests: [s3_tests.rs](../next/crates/coven-storage/src/providers/s3_tests.rs).

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

Source: [google_drive.rs](../next/crates/coven-storage/src/providers/google_drive.rs).
Tests: [google_drive_tests.rs](../next/crates/coven-storage/src/providers/google_drive_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes | **Not established:** full coven path is a Drive filename in one folder. `files.list` checks absence; `files.generateIds` assigns a new ID; resumable `files.create` publishes; `ensure_unique` selects the lowest ID and deletes the caller's duplicate. | Conformance; `concurrent_create_once_keeps_the_same_file_on_both_devices`; `two_uploads_cannot_publish_different_bytes_at_one_path`. Their completion order misses the counterexample below. Drive does not reserve the filename. |
| Replace posted positions | Implemented for a found ID: media `PATCH`; absent name goes through creation. | Conformance. Concurrent first creation and a preexisting duplicate filename are untested. |
| Read whole / range | Implemented: resolve filename with `files.list`, then `files.get?alt=media`, with `Range` for partial reads. | Conformance; end-of-upload range check. Native provider redirects, partial body failures and 1 MiB requests are untested. |
| List prefix with paging | Implemented: list the folder's untrashed children across `nextPageToken`; parse full filenames; filter prefix locally. | One-entry pages in conformance. The implementation discards duplicate filenames, so a list cannot expose the create-once violation. |
| Delete / uploader restrictions | Implemented: `ownedByMe && canDelete` permits permanent deletion; otherwise `canRemoveMyDriveParent` permits `files.update?removeParents=...`. Public `covenDevice` metadata supplies the uploader device. | `session_reopens_and_only_uploader_deletes` verifies unlinking another uploader's file without deleting its bytes. Shared-drive ownership, neither permission, missing metadata and duplicate filenames are untested. |
| Recorded resumable upload | Implemented: generated file ID and upload URL are recorded; chunk `PUT`s use `Content-Range`; empty `PUT` queries progress; a lost final reply is checked by file ID/name/size/parent. | `session_reopens_and_only_uploader_deletes`, `interrupted_part_continues_at_the_confirmed_byte`, `missing_session_and_destination_is_expired`. The filename uniqueness defect also affects publication. Abort's DELETE/499 handling has no test or Drive-specific documentation established by this audit. |
| Share an account | Implemented: paged folder `permissions.list`, create or upgrade a user permission to `writer`, then verify. | `conformance_and_account_sharing` covers a new grant. Existing reader upgrades, inherited grants, account aliases and permission paging are untested. |
| Take back access | Implemented for matching visible user permissions: `permissions.delete`, then list again. | Same test covers one direct grant. Owner removal, inherited access, groups and duplicate/overlapping invites are untested. |
| OAuth / device removal | Shared OAuth clients implement Google consent, code exchange and refresh. Sign-out instructs the member to remove the app's provider access. | Shared OAuth tests below; no provider-owner method installs refreshed tokens on an already constructed adapter. |
| Setup / probe / failures | Common implementation operates on the supplied existing folder ID. It does not create a folder or check folder identity/ownership/sharing capabilities. | Conformance only for valid writable folder. Shared HTTP tests classify a subset of Google errors. Missing container and missing object are not distinguished at the HTTP adapter. |

Google documents that a filename need not be unique within its folder in the
[File resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/files).
[Generated IDs](https://developers.google.com/workspace/drive/api/guides/create-file)
prevent duplicate creation when retrying **the same ID**; independent callers
here generate different IDs for one coven path. The
[upload guide](https://developers.google.com/workspace/drive/api/guides/manage-uploads)
documents resumable status queries and chunk alignment. Its 5 MB distinction
does not mean each resumable request is capped at 5 MB; the adapter's 8 MiB
chunks are not, by themselves, a defect.

### Dropbox

Source: [dropbox.rs](../next/crates/coven-storage/src/providers/dropbox.rs).
Tests: [dropbox_tests.rs](../next/crates/coven-storage/src/providers/dropbox_tests.rs).

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
allows editors to manage membership only when folder policy permits it, requires
ownership transfer before removing the owner, and calls out recipient mounting.
The implementation never establishes these prerequisites. Closing an upload
session is also not a documented immediate deletion of its buffered bytes.

### OneDrive

Source: [onedrive.rs](../next/crates/coven-storage/src/providers/onedrive.rs).
Tests: [onedrive_tests.rs](../next/crates/coven-storage/src/providers/onedrive_tests.rs).

| Required operation | Implementation and provider API | Existing test evidence / gap |
| --- | --- | --- |
| Create once; retry identical bytes | Implemented for nonempty data: create parent folders with conflict failure, `createUploadSession` with conflict behavior `fail`, then chunk PUTs. Empty data uses content PUT with conflict/conditional headers. | `conformance_hierarchy_pagination_and_sharing` tests nonempty conflicts. Stub's ordinary PUT overwrites unconditionally, so the separate empty-object path is not verified. |
| Replace posted positions | Implemented: parent creation and content PUT, restricted to positions. | Conformance. No atomicity/race or endpoint-size test. |
| Read whole / range | Implemented: get item metadata, then GET the preauthenticated download URL without a bearer token; range validated by common HTTP code. | Conformance; `resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls`. Redirects, expired download URLs and interrupted bodies are untested. |
| List prefix with paging | Implemented: walk child folders, follow `@odata.nextLink`, validate same origin, refuse cycles, filter parsed paths locally. | One-entry pages in conformance. Hostile/repeated links, folder cycles and malformed item types are untested. |
| Delete / deletion eligibility | Implemented: item DELETE by path; HTTP 404 succeeds. Metadata lookup precedes `DeletionRights::Delete`. | Conformance. Missing drive versus missing object and actual delete permission are not distinguished. |
| Recorded upload session | Implemented: store transfer URL, GET status without bearer, parse `nextExpectedRanges`, resume aligned parts; lost final reply falls back to destination size/name then full byte verification. | `resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls`, `lost_completion_requires_byte_verification`, `missing_session_and_destination_is_expired`. Only a single open-ended expected range is accepted; bounded/multiple ranges are untested and refused. |
| Share an account | Implemented: list permissions, POST `invite` with email/write/sign-in required, then verify. | Conformance supplies `invitation.email` and globally visible permissions. Actual recipient identities after acceptance, non-owner visibility, inherited grants and paging are untested. |
| Take back access | **Incorrect for a non-owner:** delete only email-matched permissions returned by listing; an empty result is accepted as revoked. | Stub exposes all grants to every caller, masking the documented visibility restriction. No test asserts that a removed account subsequently cannot access the folder. |
| OAuth / device removal | Shared OAuth clients use the Microsoft `consumers` endpoint, delegated file access and offline refresh; device removal gives app-revocation instructions. | Shared exchange test; work/school sign-in is not supported by the selected authority, and this restriction is not represented in `CloudProvider::OneDrive`. |
| Setup / probe / failures | Common setup assumes the drive and root folder exist. It never establishes provider ownership or sharing authority. | Conformance only. Common HTTP tests exercise selected Microsoft error codes. |

Microsoft documents [upload sessions](https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0)
with 320 KiB alignment, automatic final publication, expiry, and bounded or
multiple missing ranges. It documents the folder-sharing endpoint as
[driveItem invite](https://learn.microsoft.com/en-us/graph/api/driveitem-invite?view=graph-rest-1.0).
Most importantly, [permission listing](https://learn.microsoft.com/en-us/graph/api/driveitem-list-permissions)
returns only permissions applying to a non-owner caller. Absence from that
response is not evidence that another account has no access.

### CloudKit

Source: [cloudkit.rs](../next/crates/coven-storage/src/providers/cloudkit.rs).
Tests: [cloudkit_tests.rs](../next/crates/coven-storage/src/providers/cloudkit_tests.rs).

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
assigns private-share participant management to the owner. The adapter's
unrestricted `set_access` promise cannot give every coven admin that authority.
[CKAsset](https://developer.apple.com/documentation/cloudkit/ckasset) is fetched
as an asset with its record; arbitrary byte-range HTTP reads are not a native
CKAsset operation established here. Section 20.1 already calls for bounded
assets, so range support needs a tested native representation of those parts,
not an assertion that every CKAsset supports Range.

## Requirements shared by the providers

| Requirement | Finding and evidence |
| --- | --- |
| Exact layout (§4, §6, §9, §11, §12.2, §15, §16.2) | `ObjectPath` models device logs, store logs, positions, snapshots, sealed store/circle keys, keyed file names and join requests. `every_layout_round_trips_without_aliases` checks constructors, parsing, canonical IDs and traversal refusal. Prefixes cover all corresponding families, though there is no “all device logs” or “all store logs” prefix except `all()` plus filtering. |
| Encryption boundary (§11, §16.2, §21) | Storage accepts already encrypted bytes and `StoredFileName`; it does not calculate or send a plaintext content hash. Encryption, authentication and chunk format belong to crypto/format and callers. Their end-to-end use was not audited here. |
| Neighbouring chunks, at most 1 MiB (§16.3) | Every adapter accepts an arbitrary contiguous `ByteRange`; none knows chunk/header boundaries, caches the header or combines chunk requests. Those decisions belong to sync's file reader. No storage test requests 1 MiB or verifies a request ceiling. Whole response buffering means chunks cannot be checked before the requested response is complete. |
| Storage residence time for deletion (§15) | **Missing:** `list` returns only paths; `read` returns only bytes; no public object metadata operation exposes upload/modification time. Sync cannot learn how long storage has held an object from these operations, especially after a restore. Author timestamps cannot establish upload time. Object length is also unavailable without a read. |
| Durable session recording (§16.5, §18) | `UploadSession::encode/decode` carries provider state, location, destination, total, confirmed offset and part size. Tests recreate adapters and deserialize old values. Actual database recording belongs to sync/database and was not checked. No test covers a crash after remote session creation but before its ID is recorded. |
| Setup atomicity (§20.5) | `Storage::setup` lists the location, then creates the first entry, or accepts an identical sole entry. It commits no local settings/credentials. `StorageSettings` uses foundation's atomic file replacement. Credential/key/settings commit and rollback are facade responsibilities, not implemented by this crate. |
| Setup occupancy (§20.5) | **Race:** two callers can both list an empty location and create first entries under different device paths. There is no common atomic reservation. Existing test covers sequential retry/occupation only. Dropbox and OneDrive skip folders in listing, so a location containing only unrelated empty folders also appears empty. |
| Probe (§20.5) | Common probe checks create, whole read, range, conflict, list, delete; retains both check and cleanup failures. It does not probe positions replacement, multipart transfer, sharing authority, or recipient access. A lost create reply returns before cleanup, leaving an unrecorded probe object. Existing tests exercise only the successful path. |
| OAuth sign-in and refresh (§20.10) | `OAuthClients` builds provider-specific authorization requests with state and PKCE, exchanges codes, computes expiry with the injected clock, refreshes and retains an omitted refresh token. Browser flow binds localhost port 19284, validates the callback and responds to cancellation/deadline. Shared tests cover all three exchange providers, state/provider/redirect binding, injected time, Dropbox refresh failure/retention, network failure, local callback and cancellation. No real browser, provider registration, consent or mobile redirect was tested. |
| Installing refreshed tokens (§20.10, §21.2) | **Missing owner access:** `OAuthSession::set_tokens` exists, but each adapter consumes and privately owns its session, exposes no token-update method and does not share the session handle. A caller cannot update the constructed adapter through its capability. Reconstructing an adapter is a different operation; no test demonstrates committed refresh followed by an authenticated request on the same adapter. |
| Typed failures (§20.5, §21.3) | `StorageError`, `StorageFailure`, setup errors and OAuth errors preserve most native/HTTP causes. `ProviderResponse` and S3 error wrapper redact ordinary diagnostics. `http_tests` and `error_tests` cover selected mappings, redaction and cause retention. HTTP missing-container responses have no context-specific distinction, and Dropbox async removal converts the failure to a static protocol string, losing its original payload. |
| Member/device actions (§20.9–§20.10) | S3 manual actions and provider sign-out instructions are represented. Shared-folder grants are identified only by email, not by a recorded grant or invite. Multiple invites to one account and access predating an invite cannot be distinguished: cancellation can revoke access another invite/member still needs. No such ownership test exists. |
| Crate rules (§21) | Storage depends on foundation/crypto and external providers, not database. Production/test files are below 1,000 lines and tests are siblings. It offers memory storage, faults and conformance under `test-utils`. Public-type declarations reviewed against §20 were not changed. |

The shared sources are [storage.rs](../next/crates/coven-storage/src/storage.rs),
[session.rs](../next/crates/coven-storage/src/session.rs),
[path.rs](../next/crates/coven-storage/src/path.rs),
[http.rs](../next/crates/coven-storage/src/providers/http.rs), and
[oauth.rs](../next/crates/coven-storage/src/providers/oauth.rs).

## Provider limitations that stop implementation

The task explicitly requires: “If a provider can't do what the spec asks, stop
and report rather than inventing a workaround.” The following prevent claiming
that the existing spec can be completed solely by correcting these adapters.

1. **Drive does not supply a unique filename reservation.** The current mapping
   promises create-once coven paths but uses nonunique Drive names. A concrete
   code-level counterexample: A starts with ID `id1`; B starts with `id2`; B
   publishes first, sees only itself and reports success; A then publishes,
   becomes the lowest ID and also reports success. Future reads select A's
   bytes, despite B's earlier successful create. Both files remain because A
   deletes only its own file when it loses. A process crash between publication
   and duplicate cleanup is another exposed state. The existing tests arrange
   either the opposite completion order or both publications before lookup, so
   they do not establish this invariant. Generated IDs solve same-ID retries,
   not this cross-device path agreement. No alternative naming/coordination
   protocol was invented for the audit.

2. **Provider ownership is not coven's equal-admin model.** Section 9 allows
   several equal admins, including removal of another admin. CloudKit private
   share participation is owner-managed; Dropbox explicitly refuses removing
   the folder owner before ownership transfer. OneDrive hides other people's
   grants from non-owners, making the adapter's empty-list revocation check
   unsound. A coven role change alone changes none of those provider rights.
   The spec has no owner-preserving restriction, required transfer, or manual
   non-S3 action that resolves these cases. Merely returning `PermissionDenied`
   preserves the cause but does not fulfill the specified removal operation.

These conclusions use the provider documents linked above. They do not rely on
a live-account experiment. The limitations require a spec decision; this report
does not choose a replacement product behavior or change §20 to imply one.

There is also a limit on crash recovery that §16.5 should state: recorded
sessions remain resumable only while the provider retains them. Dropbox expires
sessions after seven days; Google and OneDrive also document expiry. The
adapters expose `SessionExpired`. This does not invalidate resumption of a
still-valid session, but recovery after expiry cannot promise to reuse its
uploaded parts. No silent restart was added.

## Additional confirmed defects and untested boundaries

These are findings for the stopped implementation, not changes included in the
report commit.

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

`Record provider constraints before completing storage` records the operation
matrix, API evidence, test gaps and provider limitations in this report. It
changes no production code, tests or public types, so §20 is unchanged. No
implementation commit was made: the provider limitations above trigger the
plan's stop condition. The commit is local, with the required co-author trailer;
no branch was pushed.

## Verification and what was not checked

The full `next/scripts/check.sh` passed with
`export PATH="$HOME/.elan/bin:$PATH"`, without overriding `CARGO_TARGET_DIR`,
`RUSTC_WRAPPER` or `CARGO_INCREMENTAL`: formatting, clippy, ownership policy,
documentation, production compilation, both test feature configurations, both
Lean proofs and the Rust/Lean differential test. All 39 storage tests passed in
each feature configuration. Passing these tests does not cover the gaps listed
above.

No live provider account, native CloudKit implementation, console-created S3
policy, real OAuth browser/consent flow, mobile platform, or non-macOS execution
was checked. This audit did not review database/sync implementations, key
custody transactions, snapshot/file retention orchestration, or the caller's
chunk-combining/cache behavior. The counterexamples above are source and
protocol findings, not newly executed reproductions. The report adds no tests
or fixes while the provider stop condition stands.
