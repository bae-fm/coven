# coven-storage

Encrypted object storage for S3, Dropbox, OneDrive and native
CloudKit on Apple platforms. This crate owns provider requests, transfer sessions, account
sharing and recipient acceptance. Sync owns writers, member/invite accounting,
retention eligibility and adjacent-chunk grouping.
Credentials and session recordings are supplied by owners; this crate does not
commit key custody or database state.

## Object and transfer contract

Every path and listing prefix begins with the canonical store UUID. Stores can
share a provider location without sharing object names, except on an S3
service whose member keys reach a whole bucket (R2, and every custom or
detected profile): there a store has the bucket to itself, at an empty base
prefix, and creating it refuses a bucket holding anything else (§4). Every path has one writer. `create` refuses an occupied path with
`AlreadyExists`; the caller counts a retry as stored only after comparing the
occupant's complete bytes (§10). Only posted
positions and per-device clock objects can be replaced. Files use foundation
`FileId` UUIDs. Prefixes include
all device logs and all store-log entries. A range is half-open and must fit
inside the complete object; callers may combine neighbouring encrypted chunks
in a request up to 1 MiB.

`single_request_limit` exposes the adapter's body threshold: S3 5 GiB,
Dropbox 150 MiB, OneDrive 250,000,000 bytes, and CloudKit 8 MiB. OneDrive uses
a decimal byte count conservatively from its published MB value; these are
adapter routing thresholds, not live measurements of server enforcement.
[OneDrive content upload](https://learn.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0).
Larger creates use
resumable or multipart transfer for every object family, including writes and
snapshots. Transfer failure aborts the unfinished session; `StorageError::Cleanup`
retains both causes if abort fails too. Positions and clock objects use one request; an
oversized replacement returns `SingleRequestTooLarge`.
For crash continuation, callers begin a session, commit its secret
recording, and retain the bytes. Resume asks the provider for confirmed progress;
`restart_upload` begins a new session at the same destination and total length
after expiry, with zero confirmed bytes; a completed session cannot restart.
Decoding validates the provider, location, state variant, nonempty identifiers,
part counts/sizes and confirmed offset. A session for another location is
refused before a request.
Sessions cannot target positions or clock objects. Lost completion replies are checked against
the immutable destination or the provider's publication identity. Abort accepts
already closed or absent sessions and preserves published objects.
OneDrive records a temporary UUID under `<store>/.coven-uploads/`, uploads there,
then publishes with a conflict-refusing server-side copy and waits for its monitor.
Successful and definitively refused copies delete the temporary; failed deletion
keeps the recording incomplete. Abort deletes it as part of session cleanup.
Published-object listings exclude this reserved namespace.

`list` follows every provider page and returns path, encrypted byte size and
provider storage time, rejecting missing or malformed metadata. Dropbox uses
`server_modified`, OneDrive
the top-level `lastModifiedDateTime`, CloudKit the publication record's server
time, and S3 `LastModified`. OneDrive copy preserves the temporary's creation
time, so its modification time records publication. AWS's native multipart
timestamp records initiation and remains a publication-time limitation (§4).
Writes are at most 1 GB (§6), below S3's 5 GiB single-request limit, so their
retention age does not depend on multipart completion time.
[AWS metadata documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html).

Setup reconnects using the store's known creation-entry path and authenticates
its contents before committing the connection. Other stores and unrelated root
content are outside the store's namespace. Provider folders inside that namespace
must be ancestors in coven's layout; empty layout folders left by interrupted
uploads contain no stored objects. The provider check removes its test object
even after an unconfirmed create and retains both operation and cleanup failures.
Changing an established store's location returns `LocationChangeUnsupported`.
Storage exposes `delete`, not a deletion-rights query: sync chooses the deleting
device under §15. A refusal retains its native cause.

## Provider operations and test evidence

Each provider has a sibling `_tests.rs` suite. HTTP providers use local servers;
CloudKit tests its real naming, metadata and NSError conversions, with ignored
[signed-host conformance tests](cloudkit-live.md) for framework operations. The shared conformance suite exercises
creation and identical retries, immutable replacement refusal, positions and clocks,
whole and range reads, prefix listing, repeated deletion, and independent stores
using the same device and entry numbers in one location.
Provider-specific suites exercise recorded sessions, lost replies, expiry,
sharing authority and error causes. Passing a stub does not prove a provider's
server-side guarantee.
S3 selects shared behaviors from the const [profile table](src/providers/s3_profile.rs).
The ignored `s3_certification` test reads `COVEN_LIVE_S3_ENV`, detects native
behavior, exercises the adapter and deletes its live objects and uploads. GCS
and R2 passed suite version 1 on 2026-10-13; AWS remains uncertified without a
live account. See the [procedure](../../spec/coven.md#4-storage-providers-and-access)
and [raw evidence](../../spec/s3-certification.md).
Every live test that reads an env file (`COVEN_LIVE_S3_ENV`,
`COVEN_LIVE_DROPBOX_ENV`, `COVEN_LIVE_DROPBOX_B_ENV`,
`COVEN_LIVE_ONEDRIVE_ENV`) reads it through one reader that fails the run on a
setting its loader does not read, so a misplaced setting is never silently
ignored. Process settings such as `COVEN_LIVE_S3_PROFILE` stay in the
environment.

| Provider | Objects and listing | Recorded uploads | Sharing and recipient access |
| --- | --- | --- | --- |
| [S3](src/providers/s3.rs) | Profile-selected conditional PUT, unconditional positions/clock PUT, `GetObject`/Range, paged listing V2 or V1, `DeleteObject`. R2 ignores checksums without stated scope. | Native multipart ID, part ETags and unique publication metadata; paged `ListParts`, conditional completion (AWS/R2) or conditional copy from a recorded temporary (GCS), and HEAD recovery. | Explicit member access key; grant/sign-out return console instructions; deletion uses pending key subjects. With admin access the profile's minter makes and deletes member keys ([`s3_minting.rs`](src/providers/s3_minting.rs); AWS and SeaweedFS IAM in [`s3_iam.rs`](src/providers/s3_iam.rs), GCS service accounts in [`s3_gcs_keys.rs`](src/providers/s3_gcs_keys.rs), R2 account tokens in [`s3_r2_keys.rs`](src/providers/s3_r2_keys.rs), RustFS service accounts in [`s3_rustfs_keys.rs`](src/providers/s3_rustfs_keys.rs)). No credential discovery. |
| [Dropbox](src/providers/dropbox.rs) | App-folder-relative or namespace-scoped strict add or overwrite, download/Range, recursive paged listing, delete. | Start/append/finish, offset recovery, immutable-byte completion verification; close for cancellation. | App folders reject sharing. Shared namespaces use owner checks, direct/inherited member pages, viewer updates and asynchronous removal. Recipient mounts the invited shared folder. |
| [OneDrive](src/providers/onedrive.rs) | Path parents, conflict-refusing content PUT, positions/clock PUT, preauthenticated download/Range without bearer, recursive paged children excluding temporaries, delete. | Recorded temporary UUID and native upload URL, bounded/multiple missing ranges, conditional copy and monitor, immutable-byte recovery verification; cancellation and temporary deletion. | Account-drive ownership check, permission pages and account identity resolution; only exclusive target-account permissions deleted. Invite carries native share token for recipient redemption. |
| [CloudKit](src/providers/cloudkit.rs) | Canonical paths as record names in a custom zone; create-once and replacement save policies, bounded CKAsset reads, paged prefix queries and server publication dates. | Recorded session UUID and 8 MiB asset parts; each part and confirmed boundary saved atomically; destination creation and session publication saved atomically. | Current account compared with zone owner; private read/write zone CKShare, participant removal, share metadata validation and native acceptance. |

OAuth uses each device's account and one presenter-based flow. The presenter
opens an authorization URL and returns the complete redirect or cancellation;
state-bound PKCE, redirect checks, code exchange and refresh stay inside coven.
The desktop presenter opens the system browser and owns the loopback listener.
iOS and Android apps supply their native sign-in sheet. Restore codes carry
provider locations, never S3 keys or OAuth tokens. Microsoft uses the common authority for personal and
work/school accounts. After custody commits refreshed tokens, the owner calls
`set_oauth_tokens`; tests verify the next request on the same adapter uses them.

Dropbox creation explicitly selects `DropboxLocation::AppFolder` or
`SharedFolder`; the app supplies the corresponding registered key. App-folder
paths omit `Dropbox-API-Path-Root` and cannot be shared. Changing that choice
requires relocation, which this API does not perform. Token custody records
the issuing client and granted access; refresh uses the same registration.

The opt-in `provider_access_live_dropbox_app_folder` test uses
`COVEN_LIVE_DROPBOX_ENV` to create, reopen and remove a store with an
App-folder app, through the production OAuth flow and adapters.

Sharing is per account. Sync must keep an account shared while a member or an
owner's open invite still needs it. Storage retains grants that also reach other
accounts and returns actionable `RetainedAccess` details. It never
removes the owner. Repeated revocation may remove a concurrent re-invitation, as
the protocol permits. Unknown account identities are reported, not guessed. An ID-only OneDrive
grant with no mapping to the requested email remains accessible and is returned
as `UnidentifiedAccount`; automatic revocation of arbitrary aliases is not
established by these tests. OneDrive resolves identities across all permission
pages and keeps email-bearing grants until dependent ID-only removals succeed,
so retries can identify them. Business/SharePoint do not return `inheritedFrom`;
the explicit-inheritance stub cases do not establish native inheritance handling
there, and native mutation refusals are preserved.
[Microsoft permission fields](https://learn.microsoft.com/en-us/graph/api/resources/permission?view=graph-rest-1.0).
`StorageInvitation` records only native recipient-acceptance material, not a
per-invite grant identity; ordinary formatting redacts its secrets.

Before changing sharing, adapters check Dropbox's folder `access_type`,
OneDrive's current-account drive id, or CloudKit's current user record id
compared with the recorded zone owner. A non-owner gets `NotStoreOwner` (`PermissionDenied`) before any
mutation. S3's console-key instructions need no account-ownership check.
Dropbox pages
direct and inherited memberships separately, removes only direct account
membership, waits for its native removal job and reports group/parent access.
Viewer upgrades use `update_folder_member`; a pending viewer without an account
id gets `AccountIdUnavailable` (`Refused`) and retains its invitation.

Joining checks invitation location and provider before onboarding, then verifies
readability. Dropbox mounts the shared namespace; OneDrive verifies the share's
drive/folder before redemption; CloudKit validates metadata before native
acceptance. Removal while joining returns the provider's permission failure;
an admin can invite again. The facade verifies coven's identity and commits keys
and credentials only after bootstrap succeeds.

`MemoryStorage::builder()` supplies a shared test location and byte-sized transfer
limits; `.clock(...)` controls publication timestamps and token expiry.
Its `Faults` can lose part or completion replies and expire pending sessions.
Completed uploads discard pending parts and retain their immutable publication
identity. `MemoryStorage::for_recipient(owner, email)` shares the backend with a
separate account and sign-in; grants and acceptance govern its reads, writes and
uploads. Revocation leaves the owner and other recipients untouched. Clones
share their account's sign-in, and replacement OAuth tokens govern subsequent
calls on the same adapter. S3 keys are outside this account-sharing fake. It
does not implement provider HTTP or substitute for the provider suites.

## Native limitations and unchecked boundaries

- Dropbox's documented viewer-update selector needs a native account ID. A
  pending viewer without that ID receives typed `AccountIdUnavailable` and
  retains the invitation. An in-place upgrade for that case has not been
  established. [Official sharing schema](https://github.com/dropbox/dropbox-api-spec/blob/main/sharing.stone).
- CloudKit calls Apple's framework through `objc2-cloud-kit` and `block2`, awaiting
  completion handlers through oneshot channels. Setup takes the entitled container
  identifier, obtains the owner and creates a store-specific custom zone. Other
  platforms return `ProviderUnavailable`. Query completeness uses the common
  one-hour listing margin; this is coven's assumption, not an Apple indexing
  deadline. The native operations and account sharing require the ignored
  [live tests](cloudkit-live.md); those have not been run.

The opt-in OneDrive live suite checks a personal account: conditional copy above
the single-request threshold, exact bytes, publication time bracketed by markers,
time stability for at least ten minutes, hidden unfinished uploads, temporary
cleanup, create-once behavior, replacement revisions and listings.
Not checked: OneDrive Business accounts, S3 console policies or SigV4 principal
isolation, real browser consent, arbitrary account aliases, mobile/non-macOS
execution, and provider versioned-bucket cleanup. Database/sync and key-custody transactions,
retention scheduling, chunk authentication/cache and chunk grouping are outside
this crate's audit.
