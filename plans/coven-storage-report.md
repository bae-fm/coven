# Storage audit implementation report

The audit was recorded before implementation. This report supersedes the
removed `next/crates/coven-storage/AUDIT.md`; the provider contract and remaining
native limitations live in the [crate README](../next/crates/coven-storage/README.md). Work is local to
`next-storage-audit`; nothing was pushed. The account-sharing and setup decisions
in the revised specification replace the audit's former stop conditions.

## Audit table

Implemented here means the Rust adapter supplies the operation and the named
stub exercises it. It does not mean a live provider account was tested.
`C` below is the shared `Conformance::run` suite, invoked by each provider:
create/retry/conflict, empty objects, immutable replacement refusal, positions,
whole/range/1 MiB reads, EOF rejection, prefix listing, repeated deletion,
probe and setup. Each provider's `_tests.rs` uses its real adapter over stub HTTP;
CloudKit uses an injected app-call stub. OAuth tests use stub token endpoints.

| Required operation | S3 | Google Drive | Dropbox | OneDrive | CloudKit |
| --- | --- | --- | --- | --- | --- |
| Create once and own-device retries | Conditional PUT; C and lost-create probe test. | Name lookup; multipart create; earliest createdTime/id copy and own duplicate deletion. C, duplicate/lost-deletion tests. | Strict add/no autorename; C. | Conflict-failing content PUT; C. | Stable record create app call; C through bridge. |
| Replace posted positions | Unconditional PUT; C. | Media PATCH or first multipart request; C and first-write request test. | Overwrite upload; C. | Content PUT; C. | Atomic replace app call; C. |
| Whole/ranged reads | GetObject/Range; C and interrupted SDK body. | Media GET/Range; C and common interrupted/short/ignored-range tests. | Download/Range; C/request counter and common HTTP tests. | Bearer-free native download URL; C and upload/download credential test. | App reads covering asset parts; C checks returned bytes, not native transfer efficiency. |
| Prefix listing and paging | ListObjectsV2; one-entry pages and malformed-cursor tests. | files.list; one-entry pages, duplicate timestamps and incomplete-search tests. | Recursive list_folder/continue; one-entry pages, folders and malformed cursors. | Recursive children/nextLink; one-entry pages, hostile links and cycles. | App owns cursor traversal; Rust checks prefix/duplicates; bridge tests. |
| Stored size/time | Size/LastModified; metadata tests. Multipart completion age remains unresolved. | size/createdTime; metadata/duplicate-copy tests. | size/server_modified; metadata tests reject client time substitution. | size/createdDateTime; metadata tests. | App publication time; bridge tests with injected clock. |
| Delete | DeleteObject; repeated deletion and native denial tests. | Delete owned objects, otherwise unlink from store folder; duplicate/lost-reply tests. | delete_v2; C and denial tests. | DELETE item; C, missing-folder distinction and denial tests. | Native delete app call; C. |
| Recorded resumable/multipart uploads | Multipart ID/ETags/token; lost replies, part paging, expiry/restart, conflicts. | Generated ID/session URL; interrupted progress, lost completion, expiry/restart. | Start/append/finish; lost offsets, byte verification, expiry/restart. | Upload session URL; bounded/multiple missing ranges, byte verification, expiry/restart. | Durable native session app calls; recreation/lost replies/expiry tests against retained bridge. |
| Single-request selection and abort | Exposed threshold, common transfer routing, native abort retries/publication preservation. | Threshold boundary, abort retries; native DELETE/499 guarantee unverified. | Threshold boundary, close/offset/gone/lost-reply abort tests. | Threshold boundary, DELETE/gone/publication tests. | Native threshold and abort contract; bridge tests. |
| Account grant/revoke | Console-key instructions only; manual-action tests. | Owner check; paged direct permissions; inherited/broad/unknown/owner grants reported. Sharing tests include upgrades and lost replies. | Owner check; direct/inherited members; viewer update and async removal. Pending viewer without account ID remains open. | Owner check; resolve native identities; delete only exclusive grants and report retained access. Multi-recipient/owner/inherited/lost-reply tests. | Native owner check and native grant/revoke result; bridge tests. |
| Recipient join | Supplied member key; no account acceptance call. | Account grant immediately usable; join lists folder. | Mount shared folder; recipient account, lost reply and revoked-access tests. | Redeem native share token after destination check; recipient/refusal/mismatch/lost-reply tests. | Invite carries share URL; app validates metadata and accepts share; separate-recipient tests. |
| Sign-in/refresh/device action | Explicit supplied credentials; sign-out returns console rotation instruction. | PKCE/code/refresh and same-adapter token update; provider app-revocation instruction. | PKCE/code/refresh and same-adapter token update; provider app-revocation instruction. | Common-authority PKCE/code/refresh and same-adapter update; app-revocation instruction. | Apple account through app; native sign-out instruction, no OAuth. |
| Setup/probe/errors | Common setup/probe; simultaneous empty setups, occupied content, lost-create cleanup and both-cause tests; SDK typed errors. | Common setup/probe, known anchor reconnect, native folder context and HTTP causes. | Common setup/probe, native namespace/member errors and job causes. | Common setup/probe, native folder context and original missing-range causes. | Common setup/probe; native error source required by app-call contract. |

Sessions are restricted to immutable paths and malformed recordings are refused.
`restart_upload` begins a fresh recording for retained bytes at the same
location/path/size. `DeletionRights` was removed; sync decides who deletes.
The memory fake models publication identity, monotonic progress, expiry,
account authority, recipient acceptance, revocation and independent sign-ins.

## Commits

Every implementation concern below passed the complete `next/scripts/check.sh`
before its local commit, and commit hooks were enabled. The check includes
formatting, clippy, ownership/layout rules, docs, production builds, both test
feature configurations, both Lean proofs and the Rust/Lean differential test.
Failures found during work were fixed and checks rerun; no failed check was
presented as passing.

| Commit | Concern and verification |
| --- | --- |
| `8b0a30a6` | Recorded the original provider audit before implementation. |
| `8fd286f7` | Added UUID FileId paths, separate snapshot audiences and prefixes for all logs; updated path fixtures and §20. |
| `65d35846` | Retained the unresolved audit in the crate and removed the superseded plan report. |
| `003a303d` | Accepted bounded/multiple OneDrive missing ranges without losing native failures or recorded progress. |
| `198acfb8` | Removed deletion-rights claims; sync determines deletion eligibility. |
| `51c33945` | Added owner-mediated OAuth token replacement and same-instance request tests. |
| `06e77793` | Kept the earliest Drive copy by provider time/ID and retried deletion of this writer's later duplicates. |
| `0d40f4cb` | Restricted sessions to immutable paths; kept first positions publication in one content request. |
| `fed477f9` | Validated serialized provider state, location, identifiers, part counts/sizes and offsets. |
| `147aeeeb` | Added restart_upload for expired sessions at the same destination and size. |
| `722476d1` | Made abort retries tolerate closed/gone sessions and Dropbox's lost-part offset. |
| `3b009062` | Exposed request thresholds and routed oversized immutable creates through sessions, preserving operation and abort errors. |
| `82928ee5` | Required the store owner's provider account before sharing changes. |
| `9d7a7055` | Upgraded Dropbox viewers in place and tested refusal/lost replies without removing access. |
| `228b8689` | Reconnected a populated store through its known first entry and retained the specified setup-race behavior. |
| `63962d0c` | Cleaned probes after lost create replies and kept both probe and cleanup causes. |
| `515599d6` | Returned StoredObject path/size/provider time, including CloudKit calls and an injected memory clock. Recorded the S3 timestamp limitation. |
| `9ce889db` | Enabled Microsoft work/school accounts through the common OAuth authority. |
| `cdb8facb` | Bound memory recovery to publication identity, removed completed pending parts and modeled expiry/lost completion. |
| `ccd6d241` | Gave S3 stub uploads distinct native IDs; tested paged parts, conflicting publication and stale aborts. |
| `f1454d12` | Deleted every Drive copy at a path, with owned deletion/nonowned unlinking and lost-reply retries. |
| `712165de` | Preserved native response bodies/headers and Dropbox job failures; added throttling, malformed-body and interrupted-read coverage. |
| `48484b5f` | Restricted OneDrive revocation to exclusive account grants; resolved native identities and reported retained multi-account/unknown/inherited/owner access. |
| `3a94bb44` | Preserved Drive inherited and broad access; tested direct/inherited upgrades, pages and lost mutation replies. |
| `ef6b71ee` | Used Dropbox's native direct/inherited membership and async removal shapes; reported retained access and pending-viewer account-ID limitations. |
| `2e5bffd5` | Added validated secret StorageInvitation recordings and recipient join, including Dropbox mount, OneDrive redemption and CloudKit acceptance app calls. |
| `f199d18a` | Modeled separate memory recipients, acceptance, independent credentials and revoked reads/writes/uploads. |
| `779c739f` | Distinguished valid EOF clipping from malformed range replies; exercised binary 1 MiB reads, empty objects and interrupted S3 bodies. |
| `ce11e5f2` | Distinguished missing Drive/OneDrive folders from missing objects; tested native permission failures across object, transfer and sharing calls and Dropbox namespace errors. |
| `4f36e958` | Refused incomplete searches, invalid cursors, native non-object items, malformed item facets, empty upload IDs/ETags and oversized recovered parts. |
| `258dd0f8` | Modeled separate Dropbox upload IDs, buffers and closed states; verified a stale abort cannot close or publish another session. |
| `0eb73420` | Chose Drive DELETE or folder unlink strictly by account ownership; retained native refusals instead of substituting a different mutation. |
| `84e694ff` | Used conservative decimal byte thresholds for Drive and OneDrive and verified routing at the boundary and next byte; updated §20. |
| `da30f115` | Kept CloudKit stub upload identities unique after recordings are forgotten; verified an old abort cannot abandon the replacement session. |
| This report’s commit | Moved the current contract and remaining limitations into crate documentation and this report, and deleted `AUDIT.md`. |

## Remaining provider constraints and verification gaps

- **S3 retention age:** AWS defines multipart `LastModified` as upload initiation,
  not completion. Listing returns that native value, explicitly documented on
  `StoredObject`; it cannot establish the completed object's 30-day residence.
  No replacement retention protocol was invented. This requirement remains
  open. [AWS metadata documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html).
- **Dropbox pending viewers:** the documented `update_folder_member` selector
  needs a native account ID, which a pending invitation need not contain. The
  adapter returns `AccountIdUnavailable` and preserves access. An in-place
  upgrade for an invitation without that ID has not been established.
  [Official sharing schema](https://github.com/dropbox/dropbox-api-spec/blob/main/sharing.stone).
- **OneDrive unidentified recipients:** permissions containing only native IDs
  cannot always be mapped to the requested email or alias. The adapter resolves
  IDs from named grants when available, retains identity-bearing grants until
  dependent removals succeed, and otherwise reports `UnidentifiedAccount`
  without deleting the ambiguous permission. Automatic removal of every such
  account remains unestablished. Business/SharePoint omit `inheritedFrom`; the
  stub tests for explicitly marked inherited permissions do not establish
  native inheritance handling for those accounts. Provider mutation refusals
  remain errors with their native causes. [Permission resource](https://learn.microsoft.com/en-us/graph/api/resources/permission?view=graph-rest-1.0),
  [identity resource](https://learn.microsoft.com/en-us/graph/api/resources/identity?view=graph-rest-1.0).
- **Drive cancellation:** retry behavior is tested against DELETE/499/404 replies.
  The current Drive documentation examined does not establish that cancellation
  guarantee. Older GData and Cloud Storage protocols are not evidence for Drive
  v3, and no live account was used to settle it.
  [Drive upload guide](https://developers.google.com/workspace/drive/api/guides/manage-uploads).

A retained share is an actionable typed result, not a claim that access was
removed. The owner/member/open-invite accounting and the permitted concurrent
re-invitation race belong to sync and were not implemented in storage.

## Verification and unchecked work

Every concern's full check included both Cargo feature configurations, clippy,
production compilation, rustdoc, owner-construction/layout checks, both Lean
proofs and the Rust/Lean differential merge test. Regression failures were
observed before the corresponding fixes. Provider tests used local HTTP or
injected CloudKit calls; no live credentials or accounts were used. The final
check passed 161 storage tests in each feature configuration. `AUDIT.md` was
removed only in the documentation commit containing this report.

Cargo initially encountered missing shared build artifacts while other worktrees
were compiling. Successful reruns used a distinct Rust metadata value and Cargo's
intermediate `CARGO_BUILD_BUILD_DIR`, without setting `CARGO_TARGET_DIR`, `RUSTC_WRAPPER`
or `CARGO_INCREMENTAL`. These environment settings were also used for
commit hooks. No hooks were skipped and no branch was pushed.

The following were not checked:

- Real provider accounts, quotas and exact server transfer limits; provider
  availability, caching or pagination consistency during concurrent changes.
  The configured thresholds route oversized bodies conservatively; native
  boundary enforcement was not measured.
- Real OAuth browser consent, application registration and mobile redirects;
  arbitrary native account aliases; S3 console-created IAM policies, SigV4
  principal isolation, third-party S3-compatible vendors, versioned-bucket or
  lifecycle cleanup.
- The app's actual CloudKit implementation: account ownership, share metadata
  validation and acceptance, durable parts across an app-process crash, bounded
  asset representation and efficient native ranged transfers. `CloudKitOps`
  states these requirements and its Rust adapter is tested against injected
  calls. [Share acceptance](https://developer.apple.com/documentation/cloudkit/ckacceptsharesoperation).
- Database/sync implementations and key-custody transactions; committing upload
  recordings and retained bytes; a crash after native session creation before
  its ID is recorded; setup settings/key rollback, encryption and authentication,
  chunk grouping/cache, retention scheduling, and competing-store detection.
- Non-macOS and mobile execution. The local check passed on this worktree's base;
  it does not certify concurrent database/sync work on other branches.
