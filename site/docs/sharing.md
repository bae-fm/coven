# Sharing

A store can have more than one writer. coven decides who may write from the
store's signed membership state: causal per-owner streams that pull verifies
against each Store commit, so the cloud provider never decides who may write.

Every initialized store has signed founder membership, including a store with
one writer and a browsable cloud home. Creation publishes a signed
`StoreProtocolRoot` that binds the store id, founder membership entry, and schema
version. Its `store_root_hash` is pinned locally and carried by every signed
Store protocol object, and the founder publishes its causal membership head.
"Browsable" describes cloud visibility and readable blob paths; it does not
disable membership authorization.

coven shares a store by **membership**: it grants the *whole store* to
another *writer*, a peer with their own identity in membership, by sealing the
store keyring to that member's keypair. The store is the unit of sharing —
a different set of people is a different store.

Examples use the todos app; two people both write todos, and the owner
controls who else can.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## Identity

A member is identified by an Ed25519 public key (32 bytes). A signature proves
who signed the bytes; accepted membership determines what that identity may do.
Devices also have registered signing keys, and their registrations must be
activated before those keys can authorize ordinary Store publications.
The key used for encryption (X25519) is computed from the Ed25519
key by
[`ed25519_to_x25519_public_key`](rustdoc:fn:coven_keys::keys::ed25519_to_x25519_public_key),
so anyone holding a member's Ed25519 public key can derive the target to wrap
the store keyring to (see [The store keyring](#the-store-keyring)).

## Membership records

Anyone who ever held bucket access can write bytes, so a correctly encrypted
but forged changeset is always possible, and there is no server to refuse it.
Each device decides who may write from storage alone, with nothing but keys to
trust. Membership changes are therefore signed records. A
[`MembershipEntry`](rustdoc:type:coven_protocol::membership::MembershipEntry)
signs a [`MembershipEntryBody`](rustdoc:struct:coven_protocol::membership::MembershipEntryBody):
the Store id, author and Owner grant, stream coordinate, previous-entry hash,
observed dependencies, resolution dependencies, display timestamp, and
[`StoreAuthorityChange`](rustdoc:enum:coven_protocol::membership::StoreAuthorityChange).
The change records a grant, removal, device exclusion proposal or outcome,
provider administrator control, or conflict resolution activation.

The signature covers the complete body.
[`verify_membership_entry`](rustdoc:fn:coven_protocol::membership::verify_membership_entry)
checks its signature and canonical resolution dependencies.

The `created_at` value is an HLC string used for display ordering, not to authorize
anything. It is author-supplied and therefore spoofable, so no access decision
reads it (see [Revocation](#revocation-is-key-rotation)).

## Membership publication

Owners keep separate, hash-linked membership streams. Entries and heads name
the author, the Owner grant authorizing that stream, and its random stream id:

```
store-v1/membership/entries/{author}/{owner_grant}/{stream_id}/{seq}/…
store-v1/membership/heads/{author}/{owner_grant}/{stream_id}/{seq}/…
```

Each entry names its predecessor and observed membership dependencies. An
`AuthorHead` selects an exact entry and links the preceding head. These links
let readers verify a stream's history; uploading an entry and head does not by
itself accept a membership change.

Except for the founder bound by the Store root, a membership head names an
activating Store commit. The commit carries the matching membership control.
Its publication enters the shared accepted history by conditionally replacing
`store-v1/publication/current.json` using the provider version the writer read.
A competing writer must observe and validate the winning history before retrying;
it cannot overwrite another accepted admission with its stale view.

After acceptance, the publisher writes a `MembershipHeadAcceptance` result
binding the exact head to the winning publication record and its verified
membership predecessor. A reader discovering membership from its rooted streams
checks that result as well as the entry, head, grant and device authority.
A missing required result leaves finalization incomplete; a signature alone
cannot turn the uploaded candidate into accepted authority.

<svg class="flow" viewBox="0 0 660 258" role="img" aria-label="Owners prepare separate membership controls, compete to advance the shared Store publication, and finalize results that readers verify">
<text class="hdr" x="105" y="22" text-anchor="middle">PREPARE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">ACCEPT</text>
<text class="hdr" x="555" y="22" text-anchor="middle">VERIFY</text>
<rect class="lanec" x="10" y="36" width="190" height="156" rx="10"/>
<rect class="lane" x="235" y="36" width="190" height="156" rx="10"/>
<rect class="lanec" x="460" y="36" width="190" height="156" rx="10"/>
<rect class="chipo" x="25" y="57" width="160" height="43" rx="7"/>
<text class="lbl s11" x="105" y="74" text-anchor="middle">Owner A</text>
<text class="sub" x="105" y="90" text-anchor="middle">entry + head + commit</text>
<rect class="chipo" x="25" y="124" width="160" height="43" rx="7"/>
<text class="lbl s11" x="105" y="141" text-anchor="middle">Owner B</text>
<text class="sub" x="105" y="157" text-anchor="middle">entry + head + commit</text>
<line class="arr" x1="200" y1="79" x2="231" y2="101" marker-end="url(#fa)"/>
<line class="arr" x1="200" y1="145" x2="231" y2="123" marker-end="url(#fa)"/>
<rect class="chipa" x="250" y="81" width="160" height="60" rx="7"/>
<text class="lbl s11" x="330" y="99" text-anchor="middle">shared current record</text>
<text class="sub" x="330" y="116" text-anchor="middle">conditional replacement</text>
<text class="sub" x="330" y="131" text-anchor="middle">one winner at a time</text>
<line class="arr" x1="425" y1="111" x2="456" y2="111" marker-end="url(#fa)"/>
<rect class="chipa" x="475" y="72" width="160" height="79" rx="7"/>
<text class="lbl s11" x="555" y="91" text-anchor="middle">accepted control</text>
<text class="sub" x="555" y="110" text-anchor="middle">exact head + result</text>
<text class="sub" x="555" y="128" text-anchor="middle">rooted authority checks</text>
<text class="sub" x="330" y="220" text-anchor="middle">A competing publication is checked against the winner before retry.</text>
<text class="sub" x="330" y="240" text-anchor="middle">Uploading signed objects alone does not accept the change.</text>
</svg>

[`MembershipChain`](rustdoc:struct:coven_protocol::membership::MembershipChain)
is derived from verified membership objects. An initialized device retains
accepted history and membership cursors in its database; loading current
membership uses that accepted publication and its retained proofs. It does not
need to rediscover all membership objects on every sync.

Validation binds the founder entry to the pinned Store root, checks exact
references and signatures, follows predecessor and dependency links, and
applies the grant and conflict-resolution rules. Store-activated controls also
require their accepted publication evidence and the issuer's device authority.
A newly signed registration does not establish its own activation.

Role changes are membership controls too. The current role comes from the
verified causal reduction, including removals and conflict resolutions, rather
than whichever signed role assignment a reader happened to load last.

## Roles

[`MemberRole`](rustdoc:enum:coven::MemberRole) has three
forms:

- **Owner** can write, and can mutate the chain: admit, remove, and change
  roles. The founder is an owner, and an owner can promote others. Any current
  owner can admit, not just the founder.
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
public key with libsodium's sealed box. The wrapping Owner also signs the
Store, recipient, generation and sealed bytes: a path under that Owner's name
does not authenticate its sender.

Wrapped keyrings are immutable exact objects at
`keys/{owner_pubkey}/{recipient_pubkey}/{generation}/{wrap_hash}.json`.
The membership control names the exact wrapped-key reference. A joiner verifies
the accepted activation, object reference and Owner signature before opening
its copy with the matching private key.

Admission retains its prepared objects and progress in a durable journal.
A failure before acceptance does not grant membership; a failure after
acceptance must resume finalization of that accepted change. It cannot undo
acceptance by deleting an entry or replacing an earlier key wrap.

## Pull verification

Each signed Store commit names the exact
[`MembershipCoord`](rustdoc:struct:coven_protocol::membership::MembershipCoord)
that grants its author write access, its device registration, and its
membership and device-state predecessors. Pull follows the accepted Store
publication history and verifies the commit's signature, exact references and
authority at that publication boundary. A device announcement helps locate
history; it does not independently accept a commit.

Required membership objects are loaded by their exact references and checked
against the pinned root and accepted control proofs. Invalid signatures,
missing required objects, inconsistent predecessors or unauthorized authors
prevent the affected history from being materialized. A commit cannot grant
its own author access merely by carrying a valid signature.

## Reading membership before opening data

A joining or restoring device needs membership authority before it can open
the store keyring. The root, membership entries, heads, acceptance results and
membership rollup are signed plaintext protocol objects. The protected snapshot
database image carries data; possessing or decrypting that image does not
establish its author's authority.

A published snapshot names a **membership rollup** carrying the membership
objects up to its declared frontier. It supplies bytes to the same rooted
verification used for individual objects. Readers still check the necessary
provider head slots and acceptance results, and follow later history; a rollup
does not make cold verification a fixed number of reads. See
[Bootstrap](bootstrap.md#the-membership-rollup).

The preliminary rollup lookup is an advisory optimization: if it cannot be
used, the rooted reader can obtain those membership objects individually.
That does not bypass a failed required proof. Once an accepted snapshot or
control is selected, its required objects and authority must verify; missing
or invalid evidence is an error.

## Revocation is key rotation

You cannot un-send data: a removed member keeps every byte they already
pulled. What removal *can* guarantee is that they read nothing new, and the
only enforcement that needs no server and no honest clock is a key they never
receive. So removal is key rotation, not a temporal replay of the chain ("was
this author allowed when they claim they wrote this?").
`handle.remove_member(...)`:

1. Prepares the removal and a new key generation, with exact wrapped keyrings
   for the remaining members, and retains them in its durable operation.
2. Uploads those immutable wraps and the removal entry, then requests removal
   of the member's cloud access. Providers with no per-member credential
   revocation report that limitation; key rotation protects new encrypted data.
3. Publishes the matching Store commit and membership head, accepts the control
   through the shared Store publication, and finalizes its acceptance result.

The removed member receives no wrap for the new generation. Earlier wraps
remain earlier-generation objects; deleting them cannot take back a key the
member already learned.

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

Subsequent Store publications are checked against the accepted removal, and
new data sealed under the rotated generation is unreadable without that key.
Remaining members keep the old generations in their keyring, so data sealed before the rotation stays
readable. This is why the timestamp does not need to be load-bearing: even a
candidate with a timestamp from before the removal cannot bypass the accepted
publication's authority checks. `remove_member` refuses to remove the last
owner.

Removal does not retract changes accepted before it. Preparing or uploading
an earlier candidate does not give that candidate an earlier acceptance time.

## One-scan device pairing

An existing device displays one pairing code. The joining device scans it,
establishes an encrypted LAN session, and sends its signed identity and provider
account through that session. After the owner approves the exact identity, cloud
access and the wrapped keyring return through the same session, encrypted to the
joining device. No second code is displayed, scanned, or pasted.

<svg class="flow" viewBox="0 0 660 216" role="img" aria-label="The owner displays one pairing code; the joining device scans it and the devices complete approval over an encrypted LAN session">
<text class="hdr" x="120" y="22" text-anchor="middle">JOINER</text>
<text class="hdr" x="330" y="22" text-anchor="middle">ENCRYPTED LAN SESSION</text>
<text class="hdr" x="540" y="22" text-anchor="middle">OWNER</text>
<rect class="lane" x="10" y="32" width="220" height="172" rx="10"/>
<rect class="lane" x="430" y="32" width="220" height="172" rx="10"/>
<circle class="numc" cx="24" cy="59" r="8"/>
<text class="num" x="24" y="62.5" text-anchor="middle">1</text>
<rect class="chip" x="30" y="46" width="180" height="26" rx="7"/>
<text class="lbl s11" x="120" y="63" text-anchor="middle">scan pairing code</text>
<line class="arr" x1="214" y1="59" x2="426" y2="59" marker-end="url(#fa)"/>
<text class="sub" x="330" y="49" text-anchor="middle">offer carries endpoint + ephemeral key</text>
<circle class="numc" cx="444" cy="108" r="8"/>
<text class="num" x="444" y="111.5" text-anchor="middle">2</text>
<rect class="chip" x="450" y="88" width="180" height="40" rx="7"/>
<text class="lbl s11" x="540" y="104" text-anchor="middle">approve_device_pairing(...)</text>
<text class="sub" x="540" y="120" text-anchor="middle">verify identity + approve</text>
<line class="arr" x1="446" y1="150" x2="234" y2="150" marker-end="url(#fa)"/>
<circle class="numc" cx="330" cy="150" r="8"/>
<text class="num" x="330" y="153.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="136" text-anchor="middle">recipient-sealed admission</text>
<circle class="numc" cx="24" cy="177" r="8"/>
<text class="num" x="24" y="180.5" text-anchor="middle">4</text>
<rect class="chip" x="30" y="164" width="180" height="26" rx="7"/>
<text class="lbl s11" x="120" y="181" text-anchor="middle">join_with_device_pairing</text>
</svg>

The owner calls `handle.start_device_pairing()`. The returned
`DevicePairingHost` owns the listener and a durable journal; its offer contains
the Store name and provider, expiry, LAN endpoints, and an ephemeral public
key. Dropping and reopening the app resumes that exact
session rather than minting another code.

After scanning, the joining device completes provider sign-in when required,
then calls `PreparedDevicePairing::open_or_create`. The pending Ed25519 identity
stays in the platform key store; the pairing journal retains the offer and
signed request but no private key. The complete request is sealed to the offer's
ephemeral key and is bound to the complete offer hash.

The owner receives the exact request from `wait_for_request()`, displays its
public-key fingerprint and provider account, and passes it with a role to
`handle.approve_device_pairing(...)`. coven:

1. grants the joiner cloud access,
2. wraps the store keyring to their X25519 key,
3. signs and validates the membership control against accepted authority,
4. accepts its Store publication and finalizes the exact head acceptance result,
5. serializes those exact admission facts and encrypts them to the request's
   pending identity,
6. binds the sealed admission to the signed transport offer for this attempt.

The sealed admission returns through the same LAN session. Only the pending
identity retained for that request can decrypt its cloud connection, Store id,
owner key, wrapped key, Store root, and membership floor. The two sides then
exchange four exact values through the Store's cloud transport:

1. The offer becomes a provider access request; the selected provider
   administrator returns an approval.
2. The approval becomes a registration request; the owner accepts it and
   returns the provider-ready bootstrap.
3. The joining device installs the snapshot database and returns its readiness
   proof; the owner verifies provider admission and returns an activation.
4. The joining device installs that activation, atomically saves its config,
   moves the pending journal into the Store database, and promotes the pending
   identity into this store's custody.

Every client call resumes the durable journal. The same exchange admits a new
member's device and another device belonging to an existing member.

The device is now a writer. A join that is interrupted keeps the exact pairing,
pending identity, membership admission, and join journals so both apps resume
the same attempt. Before approval, `PreparedDevicePairing::abandon` removes the
joining device's pending key and journal; the owner can cancel the waiting
session without admitting anyone.

The pairing code is the trust handoff. A substituted code can point at another
Store because the joining device has no earlier owner key. Once the intended
code is scanned, the signed and encrypted request/response prevent LAN traffic
from replacing either identity.

## Restore codes

A restore code recovers a store on a *new device of an existing member*,
without another admission. A device invitation adds a new identity to the chain;
a restore code re-establishes an identity that is already in it.

`handle.generate_restore_code()` encodes everything needed to reconnect into
one `coven:`-prefixed base64url string: the store id, `store_root_hash`, store
keyring, Ed25519 signing key, cloud provider, and that provider's connection
details. The
`RestoreCode` is plain
JSON under that prefix.

```text
coven:eyJ2IjozLCJzaWQiOiI1NTBl…
```

Restoring with the signing key keeps the same member identity. The recovered
device must still establish its accepted device activation before publishing;
the restored key alone does not accept a new device registration.
That identity is scoped to the one store the code names — a restore code for
store A carries no authority in any other store the same device belongs to.
`decode_restore_code`
parses the string back, and on garbled input returns a
[`RestoreCodeError`](rustdoc:enum:coven::RestoreCodeError)
(missing prefix, truncated base64, malformed JSON, or a version made by a newer
build) whose `Display` text the host can show verbatim.

A restore code deliberately omits OAuth tokens, since those expire; on a
consumer cloud the user re-authenticates during restore.
Because the code contains the store keyring and any stored credentials, it is
the most sensitive string coven produces; anyone holding it has full access to
the store.
