# CloudKit CKShare

## Current Shape

CloudKit currently behaves as a single-iCloud-account storage backend:

- `coven::CloudKitOps` exposes only flat record operations.
- `CloudKitCloudHome` always reads and writes through the same operations and rejects `grant_access`/`revoke_access`.
- bae's Swift `CloudKitService` always uses `container.privateCloudDatabase` and the fixed `bae-library` zone.
- `CloudHomeJoinInfo::CloudKit` carries no share data, so a joined library cannot distinguish a private owner home from an accepted shared home.
- membership entries record the coven public key and role, but not the provider account identity used for folder/share access. Existing OAuth backends work only because `grant_access` receives an email during invite and `revoke_access` later receives the pubkey; for Drive/Dropbox/OneDrive that means revoke cannot reliably target the provider participant when the invite was created from an email.

CKShare changes the real shape: a coven member has two identities. The public key is the cryptographic membership identity; the provider account email is the storage participant identity. They must not share one `member_id` parameter.

## Design

### Cloud Access Identity

In `coven-core/src/storage/cloud/mod.rs`, replace the `CloudHome` share methods with explicit input types:

```rust
pub struct CloudAccessGrant {
    pub member_pubkey: String,
    pub provider_account_email: Option<String>,
}

pub struct CloudAccessRevoke {
    pub member_pubkey: String,
    pub provider_account_email: Option<String>,
}

impl CloudAccessGrant {
    pub fn require_provider_email(&self, provider: &str) -> Result<&str, CloudHomeError>;
}

impl CloudAccessRevoke {
    pub fn require_provider_email(&self, provider: &str) -> Result<&str, CloudHomeError>;
}
```

`CloudHome::grant_access` takes `CloudAccessGrant`; `CloudHome::revoke_access` takes `CloudAccessRevoke`.

S3 ignores the email and still returns bucket connection info. Google Drive, Dropbox, OneDrive, and CloudKit call `require_provider_email`, so approving a provider invite without an email fails before any provider mutation. This removes the current pubkey-as-email behavior.

### Membership Provider Email

Add an optional provider email to `MembershipEntry`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub provider_account_email: Option<String>,
```

For signature compatibility:

- `canonical_bytes` keeps the exact old JSON bytes when `provider_account_email` is `None`.
- When the field is `Some`, canonical bytes include it, so the provider identity attached to an Add entry is signed by the owner.

Add `MembershipChain::current_member_provider_email(pubkey: &str) -> Option<&str>` by walking entries in order and tracking the current Add for that pubkey; Remove clears it. `revoke_member` uses that value in `CloudAccessRevoke`.

All existing founder/test constructors set `None`; `create_invitation` sets `invitee_email.map(str::to_owned)` on the Add entry.

### Invite And Revoke Ordering

`create_invitation` should compute and validate all local membership/key data before mutating provider access, then commit provider and cloud objects with rollback:

1. Convert pubkeys and build wrapped key bytes.
2. Build and sign Add entry, including `provider_account_email`.
3. Validate against the in-memory chain.
4. Call `cloud_home.grant_access(CloudAccessGrant { member_pubkey, provider_account_email })`.
5. Write wrapped key.
6. Write membership entry.

If step 5 fails, call `revoke_access` with the same identity before returning the original error. If step 6 fails, delete the wrapped key and revoke provider access before returning the original error. If rollback fails, return an error that names both the original failure and the rollback failure.

`revoke_member` should pass `CloudAccessRevoke { member_pubkey, provider_account_email }` instead of a bare pubkey. Existing operation ordering can stay, but the provider revocation now targets the same email identity that the Add entry signed.

### Join Info

Keep the existing unit variant for same-account restore/open:

```rust
CloudHomeJoinInfo::CloudKit
```

Add a distinct share variant:

```rust
CloudHomeJoinInfo::CloudKitShare {
    share_url: String,
    owner_name: String,
    zone_name: String,
}
```

`cloud_provider()` maps both variants to `CloudProvider::CloudKit`. This preserves old restore codes and old config/invite decoding for the unit variant while giving shared invites the accepted shared database coordinates.

### Config Persistence

Add the same optional CloudKit share coordinates to `CloudHomeConfig`:

```rust
#[serde(default)]
pub cloudkit_share_url: Option<String>,
#[serde(default)]
pub cloudkit_owner_name: Option<String>,
#[serde(default)]
pub cloudkit_zone_name: Option<String>,
```

`build_config` fills these fields for `CloudKitShare` and leaves them empty for private `CloudKit`. bae-core's config mirror must expose and persist these fields because bae owns the YAML file.

`sync_enabled` still accepts private CloudKit with no fields. A CloudKit config with any one share field present must require all three; missing pieces are a config error when building the CloudKit home.

### CloudKit Native API

In `crates/coven/src/storage/cloud/cloudkit.rs`, model the CloudKit home scope:

```rust
pub enum CloudKitScope {
    Private,
    Shared { owner_name: String, zone_name: String },
}

pub struct CloudKitShare {
    pub share_url: String,
    pub owner_name: String,
    pub zone_name: String,
}
```

`CloudKitCloudHome` stores `scope: CloudKitScope`.

Constructors:

- `CloudKitCloudHome::new_private(ops)`
- `CloudKitCloudHome::new_shared(ops, owner_name, zone_name)`
- Keep `new(ops)` as a private-home compatibility wrapper only if required by existing public callers; otherwise update call sites.

Extend `CloudKitOps`:

```rust
fn write_record(&self, scope: &CloudKitScope, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;
fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError>;
fn list_records(&self, scope: &CloudKitScope, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError>;
fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError>;
fn grant_share(&self, member_pubkey: &str, email: &str) -> Result<CloudKitShare, CloudHomeError>;
fn revoke_share(&self, member_pubkey: &str, email: &str) -> Result<(), CloudHomeError>;
fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError>;
```

`CloudKitCloudHome::grant_access` requires an email, calls `grant_share`, and returns `CloudHomeJoinInfo::CloudKitShare` with the returned URL and zone coordinates. `revoke_access` requires the signed provider email and calls `revoke_share`.

`build_cloud_home_for_join` handles `CloudKitShare` by calling `accept_share(share_url)`, verifying the returned `owner_name` and `zone_name` match the invite, then building `CloudKitCloudHome::new_shared`. The verification is required because record operations dispatch on known config state; if accept returns a different zone, fail loudly.

`restore` remains private CloudKit: restore codes do not carry share URLs and continue to build `CloudHomeJoinInfo::CloudKit`.

### bae Bridge

Update `bae-bridge/src/cloudkit.rs` UniFFI callback:

```rust
fn write_record(&self, owner_name: Option<String>, zone_name: Option<String>, key: String, data: Vec<u8>) -> Result<(), CloudKitError>;
fn read_record(&self, owner_name: Option<String>, zone_name: Option<String>, key: String) -> Result<Vec<u8>, CloudKitError>;
fn list_records(&self, owner_name: Option<String>, zone_name: Option<String>, prefix: String) -> Result<Vec<String>, CloudKitError>;
fn delete_record(&self, owner_name: Option<String>, zone_name: Option<String>, key: String) -> Result<(), CloudKitError>;
fn record_exists(&self, owner_name: Option<String>, zone_name: Option<String>, key: String) -> Result<bool, CloudKitError>;
fn grant_share(&self, member_pubkey: String, email: String) -> Result<BridgeCloudKitShare, CloudKitError>;
fn revoke_share(&self, member_pubkey: String, email: String) -> Result<(), CloudKitError>;
fn accept_share(&self, share_url: String) -> Result<BridgeCloudKitShare, CloudKitError>;
```

`BridgeCloudKitShare` mirrors `coven::CloudKitShare` because UniFFI cannot expose the coven type directly through the callback boundary.

The adapter converts `CloudKitScope::Private` to `(None, None)` and `Shared { owner_name, zone_name }` to `(Some(owner_name), Some(zone_name))`. It rejects one-present/one-missing scope fields before calling Swift.

Regenerate Swift bindings after changing the UniFFI interface.

### Swift CloudKitService

Update `CloudKitService` so record operations dispatch from explicit scope:

- `(ownerName: nil, zoneName: nil)` uses `container.privateCloudDatabase` and `CKRecordZone.ID(zoneName: "bae-library")`.
- `(ownerName: some, zoneName: some)` uses `container.sharedCloudDatabase` and `CKRecordZone.ID(zoneName: zoneName, ownerName: ownerName)`.
- mixed nil/non-nil scope is a `CloudKitError.Storage`.

Create the private zone only for private scope. Shared scope must not try to create the owner's shared zone.

Implement `grantShare(memberPubkey:email:)`:

1. Ensure private zone exists.
2. Fetch the private zone and verify it has `CKRecordZoneCapabilityZoneWideSharing`; fail if not.
3. Fetch or create the zone-wide share record using `CKShare(recordZoneID:)`.
4. Fetch the participant with `container.fetchShareParticipant(withEmailAddress:)`.
5. Set participant permission to read/write and add it to the share.
6. Store participant mapping on the share as `coven_member_<pubkey-prefix>` or in a `BaeCloudKitMember` record keyed by `memberPubkey`; pick the share field if CloudKit allows the value size, otherwise the side record. The mapping must be saved in the same `CKModifyRecordsOperation` as the share update when possible.
7. Save the share and return `{ share_url, owner_name, zone_name }`.

Implement `acceptShare(shareUrl:)`:

1. Fetch metadata with `CKFetchShareMetadataOperation`.
2. Accept with `CKAcceptSharesOperation`.
3. Return the share URL and the accepted share record zone's owner and zone name. For a zone-wide share, use the accepted share's `recordID.zoneID`; for metadata, root record may be absent by design.

Implement `revokeShare(memberPubkey:email:)`:

1. Fetch the zone-wide share from the private zone.
2. Find the participant by email or by the stored `memberPubkey` mapping.
3. Remove it with `removeParticipant`.
4. Save the share.
5. If the participant is absent, return success: the provider state already matches revoked.

Record read/list/write/delete must use the database and zone from the explicit scope. Shared queries must always pass the shared zone ID because CloudKit does not support cross-zone queries in `sharedCloudDatabase`.

### bae Config

Add CloudKit share fields to bae-core's config mirror and YAML conversion, matching coven names. `join_from_code` already receives the `Config` returned by coven; ensure bae persists those new fields when activating the joined library.

Private `use_cloudkit` clears any CloudKit share fields. Joining a `CloudKitShare` stores all three.

### Tests

coven-core:

- `CloudHomeJoinInfo::CloudKitShare` encode/decode round trip.
- Old `CloudHomeJoinInfo::CloudKit` round trip remains.
- `create_invitation` passes both pubkey and email to `grant_access`.
- OAuth/CloudKit-like mock fails loudly when provider email is missing.
- `MembershipEntry` with no provider email verifies old canonical bytes/signatures.
- `MembershipEntry` with provider email signs and verifies, and tampering the email fails.
- `revoke_member` passes the latest active Add email to `revoke_access`.

coven native:

- `CloudKitCloudHome` record ops pass private scope for private homes and shared scope for shared homes.
- `grant_access` returns `CloudKitShare` join info from `grant_share`.
- `join_from_invite_code` calls `accept_share` before reading the wrapped key and rejects mismatched accepted zone coordinates.
- private restore still builds private CloudKit.

bae bridge/Swift:

- Rust adapter maps `CloudKitScope` to the expected UniFFI optional owner/zone pair.
- Swift private record ops use private database and create the private zone.
- Swift shared record ops use shared database and do not create the private zone.
- Swift rejects mixed scope fields.
- Unit-test the share methods behind a test double where CloudKit objects are not constructible directly; integration testing real iCloud can stay out of CI.

Verification:

- In coven worktree: `cargo fmt --all --check`, `cargo test --workspace --all-features`.
- In bae worktree: regenerate UniFFI Swift bindings, `cargo fmt --all --check`, `cargo test -p bae-bridge --features cloudkit`, and the existing macOS build command used by the checkout hook if available.

## Non-goals

This does not add public CloudKit shares, CKShare UI presentation, CloudKit subscriptions, or Android/iOS CloudKit support. It implements private-user CKShare invitations for bae's macOS CloudKit driver and the coven provider API needed to use them.
