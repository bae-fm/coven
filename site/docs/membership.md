# Membership

A library's membership is an append-only Ed25519-signed chain. Pull verifies
both per-changeset signatures and the chain itself, so storage doesn't need
to be trusted with who is allowed to write.

## Identity

A member is identified by their Ed25519 public key. Each user generates the
keypair locally; there is no central identity server. Public keys appear in
membership entries and in changeset envelopes' `author_pubkey` field; pull
matches the two.

## The entry

```rust
pub struct MembershipEntry {
    pub action: MembershipAction,   // Add | Remove
    pub user_pubkey: String,         // member being added or removed
    pub role: MemberRole,            // Owner | Member | Follower
    pub timestamp: String,           // HLC stamp (ordering/debuggability only)
    pub author_pubkey: String,       // who signed this change
    pub signature: String,           // Ed25519 over the rest
}
```

The first entry is a self-signed `Add` for the founder as `Owner`. Every
subsequent entry must be signed by an author who is already in the chain.
Verification (`MembershipChain::verify`) walks the entries in order and
rejects the first violation.

## Library key

A library has one symmetric key. Each member has a copy wrapped to their
X25519 key (the encryption counterpart of the Ed25519 identity) and stored
under `keys/{user_pubkey}.enc` in the cloud home. Adding a member appends a
signed membership entry **and** uploads the library key wrapped to the new
member's key; the new member unwraps it locally on first sync.

## Pull validation

For each incoming changeset envelope, [`pull::pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes):

1. Verifies the envelope's signature against the embedded `author_pubkey`.
2. If a membership chain exists, checks that `author_pubkey` is a *current*
   write-capable member (Owner or Member). This is non-temporal: the check
   asks who can write *now*, not at any envelope-embedded timestamp — that
   timestamp is author-signed and therefore spoofable, so it cannot be load-
   bearing for authorization.
3. Skips the changeset on either failure (advancing the cursor so a single
   bad envelope doesn't permanently stall).

A library with no membership entries (solo, no chain established) accepts
unsigned envelopes — the chain check only fires once the chain is non-empty.

### Revocation is key rotation

Removing a member does not rely on a temporal replay of the chain. When an
owner removes a member, coven rotates the library's symmetric key and re-wraps
it for the remaining members; the removed member loses both the new key and
their `auth/keys/{pubkey}` file. A removed member cannot produce a changeset the
chain admits, encrypt against the current key, or authenticate to the proxy.
That key rotation — together with per-changeset signatures and the current-
membership check above — is what enforces revocation.

## Roles

[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) is `Owner`,
`Member`, or `Follower`. Owners can invite, remove, and rotate. Members can
read and write but cannot mutate the chain. Followers are read-only: they hold
the library key but a changeset they author is rejected on pull, and the proxy
gates their writes too. The high-level manager exposes
[`get_members`](rustdoc:method:coven::sync::sync_manager::SyncManager::get_members)
and
[`invite_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::invite_member)
for host UI flows.

## Invite and join

The two-step handshake lives in [`sync::invite`](rustdoc:mod:coven::sync::invite)
and [`sync::join`](rustdoc:mod:coven::sync::join). The joiner generates a
join request (their public keys + email); the inviter consumes it, grants
storage access via the cloud home, wraps the library key to the joiner's
X25519 key, appends a signed `Add` entry, and returns an invite code. The
joiner pastes it back and
[`join_library`](rustdoc:fn:coven::sync::join::join_library) bootstraps the
local config from the cloud home — pulling membership, unwrapping the key,
and applying the snapshot.

## Restore

Restore codes recover a configured library on a fresh device through the
cloud provider and keyring.
[`SyncManager::generate_restore_code`](rustdoc:method:coven::sync::sync_manager::SyncManager::generate_restore_code)
encodes everything needed (library id, encryption key, signing key, cloud
provider details) into a single `bae:`-prefixed base64url string;
[`decode_restore_code`](rustdoc:fn:coven::sync::restore_code::decode_restore_code)
parses it back and returns user-facing
[`RestoreCodeError`](rustdoc:enum:coven::sync::restore_code::RestoreCodeError)
on garbled input (missing prefix, bad base64, malformed JSON, version
mismatch). The `Display` strings are written for the host to show
verbatim.
