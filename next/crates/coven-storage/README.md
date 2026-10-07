# coven-storage

Encrypted object storage for S3, Google Drive, Dropbox, OneDrive and the app's
CloudKit calls. This crate owns provider requests, transfer sessions, account
sharing and recipient acceptance. Sync owns writers, member/invite accounting,
retention eligibility, setup-collision detection and adjacent-chunk grouping.
Credentials and session recordings are supplied by owners; this crate does not
commit key custody or database state.

## Object and transfer contract

Every path has one writer. `create` refuses an occupied path; `create_once`
accepts an identical retry after comparing the stored bytes. Only posted
positions can be replaced. Files use foundation `FileId` UUIDs. Prefixes include
all device logs and all store-log entries. A range is half-open and must fit
inside the complete object; callers may combine neighbouring encrypted chunks
in a request up to 1 MiB.

`single_request_limit` exposes the adapter's body threshold: S3 5 GiB,
Drive 5,000,000 bytes, Dropbox 150 MiB, OneDrive 250,000,000 bytes, and CloudKit's
app-supplied threshold. Drive and OneDrive use decimal byte counts conservatively
from their published MB values; these are adapter routing thresholds, not live
measurements of server enforcement.
[Drive uploads](https://developers.google.com/workspace/drive/api/guides/manage-uploads),
[OneDrive content upload](https://learn.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0).
Larger creates use
resumable or multipart transfer for every object family. Positions remain one
request. For crash continuation, callers begin a session, commit its secret
recording, and retain the bytes. Resume asks the provider for confirmed progress;
`restart_upload` begins a new session at the same destination after expiry.
Decoding rejects inconsistent provider/location/state/part/progress recordings.
Sessions cannot target positions. Lost completion replies are checked against
the immutable destination or the provider's publication identity. Abort accepts
already closed or absent sessions and preserves published objects.

`list` follows every provider page and returns path, encrypted byte size and
provider storage time. Setup reconnects using the known first-entry path and
bytes even after other objects have arrived. A different store or unrelated
content yields `LocationOccupied`. Two initially empty setups can both succeed;
sync detects their first entries later. Probe attempts cleanup even after an
unconfirmed create and retains both operation and cleanup failures.

## Provider operations and test evidence

Each provider has a sibling `_tests.rs` suite using local HTTP, except CloudKit,
whose injected app calls are stubbed. The shared conformance suite exercises
creation and identical retries, immutable replacement refusal, positions,
whole and range reads, prefix listing, repeated deletion, probe and setup.
Provider-specific suites exercise recorded sessions, lost replies, expiry,
sharing authority and error causes. Passing a stub does not prove a provider's
server-side guarantee.

| Provider | Objects and listing | Recorded uploads | Sharing and recipient access |
| --- | --- | --- | --- |
| [S3](src/providers/s3.rs) | Conditional `PutObject`, unconditional positions PUT, `GetObject`/Range, paged `ListObjectsV2`, `DeleteObject`. | Native multipart ID, part ETags and unique publication metadata; paged `ListParts`, conditional completion and HEAD recovery. | Explicit member access key; grant/revoke/sign-out return console instructions. No IAM calls or credential discovery. |
| [Drive](src/providers/google_drive.rs) | Full path as filename; multipart/related create, media PATCH, media GET/Range, paged `files.list`. Retry retains earliest createdTime then ID and deletes this writer's later copies. Owned objects are deleted; others are removed from the folder. | Generated file ID and resumable URL; PUT chunks/status; completion checked by native ID. | Folder-owner check; paged native permissions, direct create/update/delete; inherited/group/public/unknown access is retained and reported. Recipient uses its own OAuth account. |
| [Dropbox](src/providers/dropbox.rs) | Namespace-scoped strict add or overwrite, download/Range, recursive paged listing, delete. | Start/append/finish, offset recovery, immutable-byte completion verification; close for cancellation. | Owner check; direct/inherited member pages; in-place viewer update; asynchronous removal with native failure retained. Recipient mounts the invited shared folder. |
| [OneDrive](src/providers/onedrive.rs) | Path parents, conflict-refusing content PUT, positions PUT, preauthenticated download/Range without bearer, recursive paged children, delete. | Native upload URL, bounded/multiple missing ranges, immutable-byte completion verification; DELETE cancellation. | Account-drive ownership check, permission pages and account identity resolution; only exclusive target-account permissions deleted. Invite carries native share token for recipient redemption. |
| [CloudKit](src/providers/cloudkit.rs) | The app-call contract requires stable records, atomic saves, bounded asset reads, native paging and publication timestamps. Rust checks returned paths/range lengths. | App calls own durable parts and session identity; Rust records/validates progress. | Native owner check, grant/revoke, share URL and recipient metadata validation/acceptance through app calls. |

OAuth uses each device's account, state-bound PKCE, browser callback,
code exchange and refresh. Microsoft uses the common authority for personal and
work/school accounts. After custody commits refreshed tokens, the owner calls
`set_oauth_tokens`; tests verify the next request on the same adapter uses them.

Sharing is per account. Sync must keep an account shared while a member or an
owner's open invite still needs it. Storage retains grants that also reach other
accounts and returns actionable `MemberRemoval::AccessRemains` details. It never
removes the owner. Repeated revocation may remove a concurrent re-invitation, as
the protocol permits. Unknown account identities are reported, not guessed. An ID-only OneDrive
grant with no mapping to the requested email remains accessible and is returned
as `UnidentifiedAccount`; automatic revocation of arbitrary aliases is not
established by these tests. Business/SharePoint do not return `inheritedFrom`;
the explicit-inheritance stub cases do not establish native inheritance handling
there, and native mutation refusals are preserved.
[Microsoft permission fields](https://learn.microsoft.com/en-us/graph/api/resources/permission?view=graph-rest-1.0).
`StorageInvitation` records only native recipient-acceptance material, not a
per-invite grant identity; ordinary formatting redacts its secrets.

The memory provider uses an injected clock, separate recipient sign-ins,
account grants and acceptance, durable backend sessions, immutable publication
identity, and injected expiry/lost replies. It does not implement provider HTTP
or substitute for the provider suites.

## Native limitations and unchecked boundaries

- S3 documents multipart `LastModified` as initiation time, not completion.
  The returned timestamp cannot establish how long the complete object has
  existed for the 30-day deletion rule. No alternative retention protocol is
  implemented. [AWS metadata documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html).
- Dropbox's documented viewer-update selector needs a native account ID. A
  pending viewer without that ID receives typed `AccountIdUnavailable` and
  retains the invitation. An in-place upgrade for that case has not been
  established. [Official sharing schema](https://github.com/dropbox/dropbox-api-spec/blob/main/sharing.stone).
- Drive DELETE/499 cancellation is stub-tested; its guarantee was not
  established in the current Drive upload documentation. Older GData and Cloud
  Storage documentation do not establish modern Drive behavior.
  [Drive upload guide](https://developers.google.com/workspace/drive/api/guides/manage-uploads).
- CloudKit's real native durable implementation, bounded asset transfers and
  share acceptance must satisfy `CloudKitOps`; this crate tests the bridge
  contract, not an Apple account or app-process restart.

No live accounts, S3 console policies or SigV4 principal isolation, real browser
consent, arbitrary account aliases, mobile/non-macOS execution, or provider
versioned-bucket cleanup were checked. Database/sync and key-custody transactions,
retention scheduling, chunk authentication/cache and chunk grouping are outside
this crate's audit.
