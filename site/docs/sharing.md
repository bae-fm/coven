# Sharing

A store can have more than one writer. coven decides who may write from the
store's signed membership state. `MergeConcurrent` stores use causal owner
streams; `Serial` stores put membership changes in the same globally ordered
commit chain as row changes. Pull verifies each Store commit and the applicable
membership state, so the cloud provider never decides who may write.

Every initialized store has signed founder membership, including a store with
one writer and a browsable cloud home. Creation publishes a signed
`StoreProtocolRoot` that binds the store id, founder membership entry, schema
version, and write policy. Its `store_root_hash` is pinned locally and carried
by every signed Store protocol object. `MergeConcurrent` then publishes the
founder's causal membership head. `Serial` derives founder authorization
directly from the root and creates no membership stream or head. "Browsable"
describes cloud visibility and readable blob paths; it does not disable
membership authorization.

coven shares a store by **membership**: it grants the *whole store* to
another *writer*, a peer with their own identity in membership, by sealing the
store keyring to that member's keypair. The store is the unit of sharing —
a different set of people is a different store.

Examples use the todos app; two people both write todos, and the owner
controls who else can.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## Identity

With no server, there is nobody to hand out accounts or vouch for names, so
an identity has to prove itself: it is a keypair, and anything it signs is its
own credential. A member is an Ed25519 public key (32 bytes). Each device generates its keypair
locally; there is no identity server, and the public key is the only name a
member has. The same public key appears in two places coven cross-checks: in
membership entries, and in the `author_pubkey` field of every changeset
envelope. The key used for encryption (X25519) is computed from the Ed25519
key by
[`ed25519_to_x25519_public_key`](rustdoc:fn:coven::keys::ed25519_to_x25519_public_key),
so anyone holding a member's Ed25519 public key can derive the target to wrap
the store keyring to (see [The store keyring](#the-store-keyring)).

## Membership records

Anyone who ever held bucket access can write bytes, so a correctly encrypted
but forged changeset is always possible, and there is no server to refuse it.
Each device decides who may write from storage alone, with nothing but keys to
trust. Membership changes are therefore signed records. In a `MergeConcurrent`
store, a
[`MembershipEntry`](rustdoc:struct:coven::sync::membership::MembershipEntry)
records one change:

```rust
pub struct MembershipEntry {
    pub version: u32,
    pub store_id: String,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<MembershipCoord>,
    pub created_at: String,
    pub change: MembershipChange,
    pub signature: String,
}
```

The signature covers a deterministic serialization of every field except the
signature itself.
[`sign_membership_entry`](rustdoc:fn:coven::sync::membership::sign_membership_entry)
fills in `author_pubkey` and `signature`;
[`verify_membership_entry`](rustdoc:fn:coven::sync::membership::verify_membership_entry)
checks them.

The `created_at` value is an HLC string used for display ordering, not to authorize
anything. It is author-supplied and therefore spoofable, so no access decision
reads it (see [Revocation](#revocation-is-key-rotation)).

## MergeConcurrent: per-author-stream commitment

Two owners inviting people at the same moment must not be able to erase each
other's work. Any design with one shared "latest" object has that failure
built in: the second writer wins, the first invite vanishes. So there is no
shared object anywhere in membership. Entries live in streams identified by
their author, the Owner grant authorizing that author, and a random stream id:

```
store-v1/membership/entries/{author}/{owner_grant}/{stream_id}/{seq}/…
store-v1/membership/heads/{author}/{owner_grant}/{stream_id}/{seq}/…
```

Each owner appends entries only under a stream it created and hash-links them
(`previous_hash`). A dependency frontier names the greatest effective
coordinate in every observed stream. Dependencies are ordered by full stream
identity, so one signed byte representation exists. The owner then publishes an
[`AuthorHead`](rustdoc:struct:coven::sync::membership::AuthorHead): a signed
statement that entries `1..=seq` of that exact stream exist and that the entry at
`seq` hashes to `tip_hash`. A reader admits an author's prefix only up to that
author's head, so an entry is uncommitted until its own author's head covers
it. The committed membership set is the causally reduced union of every signed
stream prefix. If causal revocation removes a stream suffix, coven never extends
that raw stream again; its author creates and persists a fresh stream id whose
first entry depends on the effective frontier.

<svg class="flow" viewBox="0 0 660 234" role="img" aria-label="An owner appends entries under its own prefix, its signed head commits them, and readers union every current owner's committed prefix">
<text class="hdr" x="150" y="22" text-anchor="middle">OWNER A'S PREFIX</text>
<text class="hdr" x="150" y="120" text-anchor="middle">OWNER B'S PREFIX</text>
<text class="hdr" x="555" y="22" text-anchor="middle">MEMBER SET</text>
<rect class="lanec" x="10" y="32" width="440" height="66" rx="10"/>
<rect class="lanec" x="10" y="130" width="440" height="66" rx="10"/>
<rect class="chipo" x="28" y="50" width="92" height="26" rx="6"/>
<text class="lbl s11" x="74" y="67" text-anchor="middle">1 · add A</text>
<rect class="chipo" x="132" y="50" width="92" height="26" rx="6"/>
<text class="lbl s11" x="178" y="67" text-anchor="middle">2 · add B</text>
<rect class="chipo" x="236" y="50" width="92" height="26" rx="6"/>
<text class="lbl s11" x="282" y="67" text-anchor="middle">3 · add C</text>
<rect class="chipa" x="344" y="50" width="92" height="26" rx="6"/>
<text class="lbl s11" x="390" y="67" text-anchor="middle">head · 3</text>
<rect class="chipo" x="28" y="148" width="92" height="26" rx="6"/>
<text class="lbl s11" x="74" y="165" text-anchor="middle">1 · add D</text>
<rect class="chipd" x="132" y="148" width="92" height="26" rx="6"/>
<text class="lbl s11" x="178" y="165" text-anchor="middle">2 · add E</text>
<rect class="chipa" x="344" y="148" width="92" height="26" rx="6"/>
<text class="lbl s11" x="390" y="165" text-anchor="middle">head · 1</text>
<text class="sub" x="178" y="190" text-anchor="middle">uncommitted: past B's head</text>
<line class="arr" x1="440" y1="63" x2="482" y2="88" marker-end="url(#fa)"/>
<line class="arr" x1="440" y1="161" x2="482" y2="128" marker-end="url(#fa)"/>
<rect class="chipa" x="486" y="90" width="140" height="36" rx="8"/>
<text class="lbl s11" x="556" y="106" text-anchor="middle">A B C D</text>
<text class="sub" x="556" y="120" text-anchor="middle">union of committed</text>
<circle class="numc" cx="24" cy="63" r="8"/>
<text class="num" x="24" y="66.5" text-anchor="middle">1</text>
<circle class="numc" cx="336" cy="63" r="8"/>
<text class="num" x="336" y="66.5" text-anchor="middle">2</text>
<circle class="numc" cx="478" cy="108" r="8"/>
<text class="num" x="478" y="111.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="224" text-anchor="middle">1 an owner appends under its own prefix · 2 its signed head commits the prefix · 3 readers union the committed prefixes</text>
</svg>

Under `MergeConcurrent`, every owner commits under its own stream, so concurrent owners never race
each other: there is no shared last-writer-wins object one writer can overwrite
over another's entry, and a failed publish leaves at most that one author's
head behind its entries, never a wedged chain.

[`MembershipChain`](rustdoc:struct:coven::sync::membership::MembershipChain) is
rebuilt from storage on each sync, not kept in the database. Validation
enforces, in order:

1. The first entry must be an `Add` with role `Owner`, self-signed
   (`author_pubkey == user_pubkey`). This is the founder; any other shape is
   rejected. A chain whose founder is not the store's established owner is a
   takeover attempt and is refused outright.
2. Every entry must carry a valid signature and a correct `previous_hash` link
   to the previous entry in its exact author, Owner-grant, and stream-id prefix.
3. Every entry after the first must be signed by an author who is a current
   `Owner` at that point.

Re-adding an existing pubkey with a different role overwrites the old role (a
downgrade), so an owner can demote a member without removing them.

## Serial: global membership commits

`Serial` has no membership author streams or membership heads. A membership
change is a signed `StoreControl` inside the next global Store commit. The entry
names the exact hash of the membership state it changes, the commit author must
be a current Owner, and the global commit author must be a current writer.

Adding a member activates at that commit position. Removing a member and moving
to the next key generation occupy one control commit, so readers cannot observe
the removal without its key rotation. The signed global head activates the
commit with an atomic conditional update; a stale owner receives a conflict and
must rerun the operation against the current state.

## Roles

[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) has three
forms:

- **Owner** can write, and can mutate the chain: invite, remove, and change
  roles. The founder is an owner, and an owner can promote others. Any current
  owner can invite, not just the founder.
- **Member** can read and write todos, but cannot touch the chain.
- **Follower** holds the store keyring and reads everything, but may not
  write. The restriction is enforced acceptance-side: a puller re-derives each
  author's role from the chain and rejects a Follower's changesets.

`MemberRole::can_write` is true for `Owner` and `Member`, false for `Follower`.

## The store keyring

Data is only as private as the distribution of its keys: encryption means
nothing if the key travels carelessly. A store's data is encrypted under a
symmetric key that can
[rotate](#revocation-is-key-rotation); the keyring is the full set of those key
generations. Each member's copy of the keyring is sealed to their X25519
public key with libsodium's sealed box and stored under the wrapping owner's
own prefix, at `keys/{owner_pubkey}/{recipient_pubkey}.enc` in the [cloud
home](/docs/storage) — an owner writes only into its own prefix, which is what
lets a reader trust that a wrap came from that owner. Only the holder of the
matching private key can open it.

Inviting a member writes the keyring wrapped to that member at
`keys/{owner_pubkey}/{recipient_pubkey}.enc`. The wrapped keyring names the
exact membership activation: a causal entry coordinate for `MergeConcurrent`,
or a global commit position for `Serial`. A joiner unwraps it only once that
activation is visible. The new
member downloads and unwraps their copy when they join
([`unwrap_store_keyring`](rustdoc:fn:coven::sync::invite::unwrap_store_keyring)).

If any step of an invite fails partway, the steps already taken are rolled
back (a previously wrapped keyring is restored, not deleted), so a failed
invite never leaves a member half-added or an existing member locked out.

## Pull verification

In `MergeConcurrent`, each signed Store commit names the exact
[`MembershipCoord`](rustdoc:struct:coven::sync::membership::MembershipCoord)
that grants its author write access. Pull verifies the commit and device head
signatures, requires their authors to agree, and checks that coordinate against
the founder-anchored membership chain.

When a provider listing lags behind the named grant, coven reads that exact
membership object by key and verifies its hash instead of searching alternate
entries. A bad signature, relocated grant, missing grant, or signer mismatch
holds the exact Store position without applying rows. A valid commit whose
author is no longer write-capable is rejected according to the membership state
and cannot grant itself access.

In `Serial`, pull starts at the signed global head and verifies the complete
prefix in sequence. Each commit must name its exact predecessor. Membership
controls update authorization at their own commit positions; ordinary commits
are checked against the state produced by the preceding prefix. A missing
commit, package, signature, or predecessor stops the position without applying
its rows.

An initialized store never accepts an unsigned commit or a commit whose author
is unauthorized under its write policy.

## Revocation is key rotation

You cannot un-send data: a removed member keeps every byte they already
pulled. What removal *can* guarantee is that they read nothing new, and the
only enforcement that needs no server and no honest clock is a key they never
receive. So removal is key rotation, not a temporal replay of the chain ("was
this author allowed when they claim they wrote this?").
`handle.remove_member(...)`:

1. Revokes the member's cloud access: an unshare on consumer clouds; on S3,
   where one holder of a shared key cannot be cut off alone, the backend
   reports the credential as unrevocable and removal proceeds, because the key
   rotation below, not credential withdrawal, is what protects new content.
2. Commits the removal and the **new key generation** together. Under
   `MergeConcurrent` this is the removing owner's causal entry plus its rotation
   record; under `Serial` both facts are one global control commit.
3. Re-wraps the updated keyring to every remaining member under the removing
   owner's own prefix, at `keys/{owner_pubkey}/{member_pubkey}.enc`.
4. Deletes the removing owner's own wrap for the removed member, at
   `keys/{owner_pubkey}/{revokee_pubkey}.enc` — a wrap another owner sealed for
   the revokee earlier holds a pre-rotation generation, so leaving it in place
   is harmless; that owner reclaims the slot when it next rotates.

<svg class="flow" viewBox="0 0 660 158" role="img" aria-label="Removing a member appends key generation 2; remaining members receive it, the removed member stops at generation 1">
<line class="arrd" x1="30" y1="62" x2="640" y2="62" marker-end="url(#fam)"/>
<rect class="chipo" x="70" y="48" width="120" height="28" rx="7"/>
<text class="lbl s11" x="130" y="66" text-anchor="middle">generation 1</text>
<circle class="glyphf" cx="300" cy="62" r="4"/>
<text class="lbl s11" x="300" y="40" text-anchor="middle">remove member</text>
<rect class="chipa" x="400" y="48" width="120" height="28" rx="7"/>
<text class="lbl s11" x="460" y="66" text-anchor="middle">generation 2</text>
<text class="sub" x="130" y="102" text-anchor="middle">everyone could read</text>
<text class="sub" x="460" y="102" text-anchor="middle">re-wrapped to remaining members only</text>
<text class="sub" x="330" y="134" text-anchor="middle">the removed member's keyring stops at generation 1: new data is unreadable to them</text>
</svg>

After this, the removed member is no longer a current member (anything they
sign is rejected against the chain), and everything sealed after the rotation
is under a generation they never receive. Remaining members keep the old
generations in their keyring, so data sealed before the rotation stays
readable. This is why the timestamp does not need to be load-bearing: even a
changeset with a timestamp from before the removal cannot be admitted, because
it would be signed by a non-member. `remove_member` refuses to remove the last
owner.

Removal does not retract old changesets the member already authored; pull
stops admitting new ones.

## Invite and join

An invite has to move two different things safely: the joiner's identity to
the owner (so the keyring can be wrapped to it), and cloud access plus the
wrapped keyring back to the joiner. A two-step handshake moves each in the
right direction, and neither side ever types a key by hand.

<svg class="flow" viewBox="0 0 660 216" role="img" aria-label="The joiner sends a join request code; the owner grants access and returns an invite code; the joiner bootstraps from it">
<text class="hdr" x="120" y="22" text-anchor="middle">JOINER</text>
<text class="hdr" x="330" y="22" text-anchor="middle">OUT OF BAND</text>
<text class="hdr" x="540" y="22" text-anchor="middle">OWNER</text>
<rect class="lane" x="10" y="32" width="220" height="172" rx="10"/>
<rect class="lane" x="430" y="32" width="220" height="172" rx="10"/>
<circle class="numc" cx="24" cy="59" r="8"/>
<text class="num" x="24" y="62.5" text-anchor="middle">1</text>
<rect class="chip" x="30" y="46" width="180" height="26" rx="7"/>
<text class="lbl s11" x="120" y="63" text-anchor="middle">join request code</text>
<line class="arr" x1="214" y1="59" x2="426" y2="59" marker-end="url(#fa)"/>
<text class="sub" x="330" y="49" text-anchor="middle">carries the joiner's public key</text>
<circle class="numc" cx="444" cy="108" r="8"/>
<text class="num" x="444" y="111.5" text-anchor="middle">2</text>
<rect class="chip" x="450" y="88" width="180" height="40" rx="7"/>
<text class="lbl s11" x="540" y="104" text-anchor="middle">invite_member(...)</text>
<text class="sub" x="540" y="120" text-anchor="middle">Add entry + wrapped keyring</text>
<line class="arr" x1="446" y1="150" x2="234" y2="150" marker-end="url(#fa)"/>
<circle class="numc" cx="330" cy="150" r="8"/>
<text class="num" x="330" y="153.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="136" text-anchor="middle">invite code: cloud access + store id</text>
<circle class="numc" cx="24" cy="177" r="8"/>
<text class="num" x="24" y="180.5" text-anchor="middle">4</text>
<rect class="chip" x="30" y="164" width="180" height="26" rx="7"/>
<text class="lbl s11" x="120" y="181" text-anchor="middle">join_from_invite_code</text>
</svg>

The joiner runs `generate_join_request`, which mints a fresh Ed25519 keypair
— scoped to this one join, not yet to any store — and produces a base64url
code carrying its public key (and, for folder-sharing providers, the account
email the owner should share to). They send it to the owner out of band.

The owner calls `handle.invite_member(...)` with that public key and a role.
coven:

1. grants the joiner cloud access,
2. wraps the store keyring to their X25519 key,
3. signs and validates the membership change against the committed state,
4. activates the change through the owner's causal head or the Serial global
   head.

The cloud connection details come back packed with the store id, name, owner
pubkey, wrapped-key author, `store_root_hash`, and policy-shaped membership floor into an
[`InviteCode`](rustdoc:struct:coven::join_code::InviteCode). The owner sends
that back.

The joiner pastes the invite code, alongside the join-request code it kept
from step 1, into
[`join_from_invite_code`](rustdoc:fn:coven::sync::join::join_from_invite_code),
which:

1. decodes both codes and builds the cloud connection (running any OAuth flow
   inline),
2. unwraps the store keyring,
3. bootstraps the local database from the latest snapshot,
4. pulls the changesets created since that snapshot,
5. promotes the pending identity from step 1 into this store's own identity
   — scoped to this store alone; a device's identity in any other store it
   belongs to is untouched,
6. saves the new store config.

The join call also receives the host's expected `WritePolicy`; a mismatch is
refused before provider access or local writes. A custom S3 `Serial` join must
add `Some(CustomS3Serial::ConditionalPutAndStrongReads)` so the local operator,
not the invite, asserts that endpoint's conditional-write and read behavior.

The device is now a writer. A join that fails partway never deletes a store
that already existed on the device, and leaves the pending identity from
step 1 untouched — the same join can be retried with the same request code.

The invite code carries plaintext cloud credentials (for S3, the access key and
secret). Treat it with the same secrecy as the encryption key, and send it over
a private channel.

## Restore codes

A restore code recovers a store on a *new device of an existing member*,
without anyone re-inviting them. Where an invite code adds a new identity to the
chain, a restore code re-establishes an identity that is already in it.

`handle.generate_restore_code()` encodes everything needed to reconnect into
one `coven:`-prefixed base64url string: the store id, `store_root_hash`, store
keyring, Ed25519 signing key, cloud provider, and that provider's connection
details. The
[`RestoreCode`](rustdoc:struct:coven::sync::restore_code::RestoreCode) is plain
JSON under that prefix.

```text
coven:eyJ2IjozLCJzaWQiOiI1NTBl…
```

Restoring with the signing key keeps the same Ed25519 identity, so the
recovered device is still the same member in the chain and can keep writing.
That identity is scoped to the one store the code names — a restore code for
store A carries no authority in any other store the same device belongs to.
[`decode_restore_code`](rustdoc:fn:coven::sync::restore_code::decode_restore_code)
parses the string back, and on garbled input returns a
[`RestoreCodeError`](rustdoc:enum:coven::sync::restore_code::RestoreCodeError)
(missing prefix, truncated base64, malformed JSON, or a version made by a newer
build) whose `Display` text the host can show verbatim.

Restore likewise requires the expected `WritePolicy` and the custom-S3 Serial
assertion when applicable; a code cannot silently select either on the host's
behalf.

A restore code deliberately omits OAuth tokens, since those expire; on a
consumer cloud the user re-authenticates during restore.
Because the code contains the store keyring and any stored credentials, it is
the most sensitive string coven produces; anyone holding it has full access to
the store.
