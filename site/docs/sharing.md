# Sharing

A library can have more than one writer. Coven decides who may write by an
append-only, Ed25519-signed log of membership changes, the membership chain.
Pull verifies both each changeset's own signature and the chain itself, so the
cloud provider never has to be trusted with who is allowed to write.

Coven shares a library by **membership** — most of this page: it grants the
*whole library* to another *writer*, a peer with their own identity in the chain,
by sealing the library key to that member's keypair (asymmetric `seal_box`).
[Item keys](#item-keys) are a separate, finer scope — a per-item key, independent
of the library master, that gives one item its own encryption key.

The examples use a todos app: `workspaces` hold `lists`, a `list` holds
`todos`, and a todo can carry `todo_attachments`. Two people sharing the
library both write todos; the owner controls who else can.

## Identity

A member is an Ed25519 public key (32 bytes). Each device generates its keypair
locally; there is no identity server, and the public key is the only name a
member has. The same public key appears in two places coven cross-checks: in
membership entries, and in the `author_pubkey` field of every changeset
envelope. The key derived for encryption (X25519) is computed from the Ed25519
key by
[`ed25519_to_x25519_public_key`](rustdoc:fn:coven::keys::ed25519_to_x25519_public_key),
so anyone holding a member's Ed25519 public key can derive the target to wrap
the library key to (see [Library key](#library-key)).

## The membership entry

A [`MembershipEntry`](rustdoc:struct:coven::sync::membership::MembershipEntry)
records one change:

```rust
pub struct MembershipEntry {
    pub action: MembershipAction,  // Add | Remove
    pub user_pubkey: String,       // member being added or removed
    pub role: MemberRole,          // Owner | Member | Follower
    pub timestamp: String,         // HLC stamp, for ordering only
    pub author_pubkey: String,     // who signed this change
    pub signature: String,         // Ed25519 over the canonical bytes
}
```

The signature covers a deterministic serialization of every field except the
signature itself, produced by
[`canonical_bytes`](rustdoc:fn:coven::sync::membership::canonical_bytes) (a JSON
object with keys in a fixed order).
[`sign_membership_entry`](rustdoc:fn:coven::sync::membership::sign_membership_entry)
fills in `author_pubkey` and `signature`;
[`verify_membership_entry`](rustdoc:fn:coven::sync::membership::verify_membership_entry)
checks them. Entries live in storage at
`membership/{author_pubkey_hex}/{seq}.enc`, one file per entry, encrypted with
the library key.

The `timestamp` is an HLC string used to order the chain, not to authorize
anything. It is author-supplied and therefore spoofable, so no access decision
reads it (see [Revocation](#revocation-is-key-rotation)).

## The chain

[`MembershipChain`](rustdoc:struct:coven::sync::membership::MembershipChain) is
the log of those entries. It is rebuilt from storage on each sync, not kept in
the database.
[`MembershipChain::from_entries`](rustdoc:method:coven::sync::membership::MembershipChain::from_entries)
sorts entries by timestamp and validates the whole chain;
[`MembershipChain::add_entry`](rustdoc:method:coven::sync::membership::MembershipChain::add_entry)
validates and appends one. Both enforce the same rules that
[`MembershipChain::validate`](rustdoc:method:coven::sync::membership::MembershipChain::validate)
applies to a full chain:

1. The first entry must be an `Add` with role `Owner`, self-signed
   (`author_pubkey == user_pubkey`). This is the founder; any other shape is
   rejected.
2. Every entry must carry a valid signature.
3. Every entry after the first must be signed by an author who is a current
   `Owner` at that point in the chain.

Validation walks the entries in order, tracking the active member set as it
goes, and rejects the first violation. Re-adding an existing pubkey with a
different role overwrites the old role (a downgrade), so an owner can demote a
member without removing them.

## Roles

[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) has three
forms:

- **Owner** can write, and can mutate the chain: invite, remove, and change
  roles. The founder is an owner, and an owner can promote others.
- **Member** can read and write todos, but cannot touch the chain.
- **Follower** holds the library key and reads everything, but may not write. A
  changeset a follower authored is rejected on pull, and the proxy gates their
  writes too.

`MemberRole::can_write` is true for `Owner` and `Member`, false for `Follower`.
`MemberRole::as_str` gives the lowercase wire string (`owner`, `member`,
`follower`) written into the per-member proxy files described below.

## Library key

A library has one symmetric key that encrypts all its data. Each member gets
their own copy of that key, sealed to their X25519 public key with libsodium's
sealed box (`keys::seal_box_encrypt`) and stored at `keys/{pubkey}.enc` in the
cloud home, keyed by the member's Ed25519 public key. Only the holder of the
matching private key can open it.

Inviting a member writes two things: the signed `Add` entry, and the library
key wrapped to that member at `keys/{pubkey}.enc`. The new member downloads and
unwraps their copy when they join with
[`unwrap_library_key`](rustdoc:fn:coven::sync::invite::unwrap_library_key),
decrypting it with their X25519 private key.

Alongside the wrapped keys, coven writes one plaintext file per member at
`auth/keys/{pubkey}` containing the member's role string. These let an HTTP
proxy gate writes by role without decrypting the membership chain;
`membership_ops::sync_authorized_keys` reconciles them against the current
chain (writing files for current members, deleting files for removed ones).

## Pull verification

For each incoming changeset envelope,
[`pull::pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes):

1. Verifies the envelope's signature against its embedded `author_pubkey`.
2. If the library has a membership chain, checks that `author_pubkey` is a
   current write-capable member, via
   [`MembershipChain::can_write_now`](rustdoc:method:coven::sync::membership::MembershipChain::can_write_now).
   Coven always signs at creation, so an unsigned or non-member envelope at this
   point is forged and is dropped.
3. On either failure, skips the changeset and advances the cursor past it, so
   one bad envelope does not stall the puller forever.

A library with no membership entries (one person, no chain established yet)
accepts unsigned envelopes; the chain check only runs once the chain is
non-empty.

`can_write_now` asks the current chain, not the envelope's timestamp. The
timestamp is author-signed and so untrustworthy for authorization; the question
that matters is "is this author allowed to write *now*", and that is answered
against the latest chain coven holds.

## Revocation is key rotation

Removing a member is not a temporal replay of the chain ("was this author
allowed when they claim they wrote this?"). It is enforced by rotating the key.
[`revoke_member`](rustdoc:fn:coven::sync::invite::revoke_member), reached from
the host as
[`SyncManager::remove_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::remove_member):

1. Revokes the member's cloud access (a no-op on S3, an unshare on consumer
   clouds).
2. Signs and appends a `Remove` entry.
3. Generates a fresh library key with `encryption::generate_random_key`.
4. Re-wraps the new key to every remaining member at their `keys/{pubkey}.enc`.
5. Deletes the removed member's wrapped key.

After this, the removed member is no longer a current member (so
`can_write_now` rejects anything they sign), and they cannot encrypt against or
decrypt the new data, because they never receive the rotated key. This is why
the timestamp does not need to be load-bearing: even a changeset with a
timestamp from before the removal cannot be admitted, because it would be signed
by a non-member and encrypted with a retired key. `revoke_member` refuses to
remove the last owner.

Removal does not retract old changesets the member already authored; pull
stops admitting new ones.

## Invite and join

Sharing a library with a new device is a two-step handshake, so neither side
has to enter the other's keys by hand.

The joiner runs `join_code::generate_join_request`, producing a base64url
[`JoinRequestCode`](rustdoc:struct:coven::join_code::JoinRequestCode) that
carries their Ed25519 public key (and an optional email). They send it to the
owner out of band.

The owner calls
[`SyncManager::invite_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::invite_member)
with that public key and a role. Under it,
[`create_invitation`](rustdoc:fn:coven::sync::invite::create_invitation) grants
the joiner cloud access, wraps the library key to their X25519 key, signs and
validates an `Add` entry against the local chain *before* writing anything, then
uploads the wrapped key and the entry. It returns the cloud connection details,
which `invite_member` packs together with the library id, name, and owner pubkey
into an [`InviteCode`](rustdoc:struct:coven::join_code::InviteCode). The owner
sends that back.

The joiner pastes the invite code into
[`join_from_invite_code`](rustdoc:fn:coven::sync::join::join_from_invite_code),
which decodes it, builds the cloud connection (running any OAuth flow inline),
and calls
[`join_library`](rustdoc:fn:coven::sync::join::join_library): it unwraps the
library key, bootstraps the local database from the latest snapshot, pulls the
changesets created since that snapshot, and saves the new library config. The
device is now a writer.

The invite code carries plaintext cloud credentials (for S3, the access key and
secret). Treat it with the same secrecy as the encryption key, and send it over
a private channel.

## Restore codes

A restore code recovers a library on a *new device of an existing member*,
without anyone re-inviting them. Where an invite code adds a new identity to the
chain, a restore code re-establishes an identity that is already in it.

[`SyncManager::generate_restore_code`](rustdoc:method:coven::sync::sync_manager::SyncManager::generate_restore_code)
encodes everything needed to reconnect into one `coven:`-prefixed base64url
string: the library id, the library key, the Ed25519 signing key, the cloud
provider, and that provider's connection details. The
[`RestoreCode`](rustdoc:struct:coven::sync::restore_code::RestoreCode) is plain
JSON under that prefix.

```text
coven:eyJ2IjoxLCJsaWQiOiI1NTBl…
```

Restoring with the signing key keeps the same Ed25519 identity, so the
recovered device is still the same member in the chain and can keep writing.
[`decode_restore_code`](rustdoc:fn:coven::sync::restore_code::decode_restore_code)
parses the string back, and on garbled input returns a
[`RestoreCodeError`](rustdoc:enum:coven::sync::restore_code::RestoreCodeError)
(missing prefix, truncated base64, malformed JSON, or a version made by a newer
build) whose `Display` text the host can show verbatim.

A restore code deliberately omits OAuth tokens, since those expire; on a
consumer cloud the user re-authenticates during restore.
`restore_code::provider_needs_oauth` reports which providers require that step.
Because the code contains the library key and any stored credentials, it is the
most sensitive string coven produces; anyone holding it has full access to the
library.

## Item keys

An **item key** is a random 32-byte key for one *item* — a logical unit the host
names (a todo with its attachments, a music release with its files), identified by
an opaque `item_id`. It is the second key tier, below the library master key.

Coven owns its lifecycle. The host calls
[`mint_item_key(item_id)`](rustdoc:method:coven::database::Database::mint_item_key) when it
creates the item; coven generates the key and stores it in the synced `item_keys`
table. Because that table is synced like any other, the key rides the
master-encrypted changeset to every member and is preserved in snapshots — a
member who joins by changeset replay *or* by snapshot bootstrap gets it. The host
never sees raw key bytes: it tags a blob with
[`BlobScope::Item(item_id)`](rustdoc:enum:coven::blob::BlobScope) (see
[Blobs](blobs.md#encryption-scope)), and coven resolves the id to the key when it
encrypts on push and decrypts on pull.

**Why an item key and not `Master` or `Derived`?** Because it is **independent of
the master** — a random key coven mints, stores, and syncs, not one derived from
the master. It gives one item its own key without reusing the library master or a
master-derived key.

A *member* can read every item key (each rides the master-encrypted changeset, and
a member holds the master), so an item key does not hide an item *from members* —
it scopes one item to a key of its own.

Item keys are opt-in. An app that needs no per-item key never emits
`BlobScope::Item` and never calls `mint_item_key`: it stays on `Master`/`Derived`
and the `item_keys` table stays empty.

