# Threat model

coven has no server. A store's data lives in the user's own cloud bucket, and
every device that writes it holds the store key. The storage provider is not
trusted with plaintext, and it is not trusted to say who may write. This page
states, per adversary, what coven defends and what it does not — so the line
between "coven's job" and "the deployment's job" is written down rather than
re-derived each time.

Two mechanisms carry most of the defense, and the sections below refer back to
them:

- **The store keyring.** A set of symmetric keys shared by every member, each
  identified by its fingerprint and ordered by a generation number, used with
  XChaCha20-Poly1305 (see [Encryption](/docs/encryption)). Every sealed object
  names the key that sealed it by fingerprint, adoption merges keyrings rather
  than replacing them, and every device seals new content under the same
  deterministically chosen key — so two owners rotating concurrently converge
  instead of splitting the store.
  Every changeset, snapshot, blob, and membership record is sealed under it. The
  cipher is authenticated: a flipped byte fails to open. The associated data of
  each sealed object binds it to its `(store_id, cloud_key)` position, so an
  object copied to a different key fails to open there.
- **Signed membership.** Causal, per-author membership streams beginning at the
  founder bound into the signed Store protocol root (see
  [Sharing](/docs/sharing)). Heads, snapshot metadata, and wrapped keys are
  signed, so the provider holding them cannot forge membership.

## Non-member with read access to the bucket

Someone who can read the bucket but is not a member — a leaked read credential, a
provider employee, a misconfigured public bucket.

**Defended.** Confidentiality comes from the store keyring: every object is
sealed before it leaves the device, and the reader holds no generation of that
key, so the bucket is ciphertext and a flat set of key paths. Integrity of the
control plane comes from the signatures on membership entries, heads, snapshot
metadata, and wrapped keys — a reader who cannot write changes nothing, and a
reader learns nothing beyond object sizes and access timing.

## Non-member with write access to the bucket

Someone who can write the bucket but is not a member — a leaked write credential,
or the provider itself acting maliciously (see also
[the provider](#the-storage-provider-itself)).

**Tampering is detected; availability is not.** A write-capable attacker can
replace or delete any object, but cannot produce one that passes verification:
membership entries and control objects carry Ed25519 signatures the attacker
cannot forge, and sealed content is bound by its associated data to the exact
`(store_id, cloud_key)` it was written under, so an authentic object relocated to
another key fails to open there. A changeset additionally carries its
`(device_id, seq)` inside the signature, and the puller refuses one whose
declared position does not match where it was fetched from — before the signature
or schema is even consulted (see
`pull_changes`) — so a member's
authentic changeset cannot be replayed into another slot.

Availability is *not* defended: anyone who can write the bucket can also delete
it. coven detects the resulting corruption or absence and refuses to act on it;
it cannot prevent the loss. Keeping bucket write credentials to the members who
need them is the deployment's responsibility.

## Current member

A device that is currently in signed membership with a write-capable role.

**Defended by authorization.** A current Owner or Member may write changesets; a
Follower is read-only. The puller judges every Store commit against the applicable membership state: an
author who is not a write-capable member at pull time is rejected, and the
snapshot (a destructive, whole-catalog primitive) is authorized to Owners only.
Membership is anchored to its founder, so a member cannot rewrite history
to grant themselves a role they were never given. What a current member *can* do
— they hold the store key and a valid identity — is exactly what membership
grants: write store data. Removing that capability is the next section.

## Current member, against another member's Circle

A current store member asking what they learn about a [Circle](/docs/circles)
they are not in.

**Circle contents are defended; the Circle's existence and administration are
not.** A Circle package is encrypted to that Circle's key, which a non-member
never receives, so its rows, roster, and display name stay confidential. What a
store member *does* see, because it is Store-encrypted control state they must be
able to verify: that the Circle exists, the Owner public keys and head author
coordinates that administer it, and — through the Store-visible routing mirror —
pseudonymous per-Circle row counts and the timing of every audience move. The
administrator visibility is deliberate: verifying a Circle's close outcome,
conflict resolution, or deletion requires reading the Owner authority that signed
it. A store member who holds the routing key can also test guesses for
predictable table-and-primary-key combinations against the mirror. These are
stated protocol properties, listed in full under
[the Circle privacy limits](/docs/circles#security-and-privacy-limits); the
boundary that hides even counts and administrators is a separate store, not a
Circle.

## Revoked member with residual bucket access

A former member who has been removed from the chain but still holds a bucket
credential the provider has not withdrawn.

**Confidentiality of new content is defended; integrity and availability of the
control state are not.** Removal rotates the store keyring: it appends a fresh
generation, re-wraps it for every remaining member, and never wraps it for the
removed one (see [revocation is key rotation](/docs/sharing#revocation-is-key-rotation)).
Content written after the rotation is sealed under a generation the removed member
never receives. This holds even when the removing device cannot immediately adopt
the new generation locally: a device that knows a rotation has committed but has
not folded it into its own cipher refuses to seal anything new for the cloud
until it adopts it (see
[`RotationPending`](rustdoc:struct:coven_protocol::objects::RotationPending)),
so there is no window in which the store keeps producing content under the
superseded generation the removed member still holds.

What is *not* defended, for a removed member who keeps a working write credential:
the integrity and availability of the control plane. They can still delete or
withhold objects (they can write the bucket), and their tampering is detected but
not prevented — this is the [non-member-with-write](#non-member-with-write-access-to-the-bucket)
case, and coven's responsibility ends at detection.

Whether the residual credential exists at all is provider-dependent. Consumer
clouds (Google Drive, Dropbox, OneDrive) and CloudKit unshare the folder or zone
on removal, so the credential stops working and this adversary ceases to exist.
S3 hands out one static bucket credential that cannot be withdrawn from a single
member; there, revoking the *credential* is the bucket administrator's job, and
coven's guarantee is the key rotation above, not the withdrawal. Member removal
completes on an S3 home for exactly this reason — it does not wait on a
credential revocation the provider cannot perform.

## The storage provider itself

The provider that holds the bucket, in full generality: it serves every read,
accepts every write, and chooses what to return.

**Untrusted for content and for the control plane; it can still withhold,
reorder, and roll back.** Content is sealed and control objects are signed, so
the provider cannot read plaintext or forge a membership decision. What it
retains is scheduling power over what it serves: it can withhold an object,
serve an older version, or reorder what a reader sees.

Rollback of *membership* is bounded: readers persist per-author-stream head
watermarks and refuse regressions. Device invitations and restore codes carry the
corresponding causal frontier, so a fresh device refuses membership older than
the code it received (see [Bootstrap](/docs/bootstrap)).

Withholding is *not* defendable from inside the artifact. A provider that stops
serving new objects presents a reader with a stale-but-internally-consistent
view, and nothing in the bucket distinguishes "nothing new" from "new things
hidden." Detecting that requires an external freshness anchor coven does not
have; it is out of scope.

## A local attacker with write access to the store directory

Someone who can write the on-device store directory — its SQLite database, its
`sync_state`, its keyring envelope.

**Out of scope.** coven trusts its own local store. The head watermark, the
pinned owner, and the device identity all live there; an attacker who can rewrite
`sync_state` can clear the rollback floor, and one who can rewrite the database
can substitute local data wholesale. The local device's own protection — full-disk
encryption, OS process isolation, the platform keychain that holds the store key
(see [Keys](/docs/keys)) — is the boundary here, not coven. The passphrase
envelope is the one piece coven treats as untrusted input from disk, and only
narrowly: it reads the envelope's KDF parameters from an unauthenticated file, so
it floors them rather than deriving a key at whatever cost the file names.

## The device-pairing channel

The existing device displays one pairing code in person. It contains an
ephemeral public key, LAN endpoints, provider identity,
and expiry. The joining device seals its signed identity request to that key;
the existing device accepts one exact request for the session and refuses a
competing identity. Its credential-bearing response is sealed again to the
approved joining identity.

**The displayed code is the trust handoff.** A replacement code can point the
joining device at an attacker's Store because the joining device has no earlier
owner key to compare. Once scanned, changing LAN traffic cannot substitute
either side: the request is bound to the complete offer, and
the returned admission is bound to the request's public key. The admission's
founder key is pinned for every later membership decision.

## Cross-store linkability

A provider that hosts two of one user's stores, asking whether it can tell they
belong to the same device.

**Defended.** A device's signing identity is per store: each store it belongs to
gets its own freshly generated Ed25519 keypair, established when that store is
created, joined, or restored, never derived from a shared secret and never reused
across stores (see [Keys](/docs/keys#no-silent-identity-minting)). No pubkey a
device writes into one store's membership state appears in another's, so a
provider hosting two of a user's stores cannot tie them to one device by matching
signing keys. A per-store restore code likewise carries authority scoped to the
one store it names.

## Accepted risks

Four positions are worth stating on their own, as deliberate scope decisions
rather than gaps to close later.

**Removal either activates its complete state or restores the prior state.**
Removing a member records the prior provider access and wrapped-key objects,
then publishes the membership removal with its replacement key generation. If
activation loses its expected head or another step fails, coven restores those
exact prior objects and access grants and reports the failure.

**A peer's tombstone GC can outrace a re-upload's cancel.** When a blob is
re-uploaded to a cloud key whose deletion tombstone has already passed its grace
(seven days), the re-uploader cancels the tombstone — but a *peer* whose database
has seen the deletion and not yet the re-share judges the blob dead, and its GC
can reclaim it before the cancel lands. The device-local pending-cancel gate
cannot cover this: it lives in each device's own database. The grace period is
the bound — the race needs a tombstone already past it at re-upload time — and
the failure is loud, never silent: the re-uploader's push refuses to publish a
row whose blob is missing remotely and retries every cycle. Hosts that write new
content at new (content-addressed or versioned) blob keys never re-enter a
tombstoned key and avoid the race entirely.

**Withholding and local-state tampering are out of scope**, for the reasons in
[the provider](#the-storage-provider-itself) and
[the local attacker](#a-local-attacker-with-write-access-to-the-store-directory)
sections: the first needs an external freshness anchor coven does not have, and
the second is the device's own protection boundary, not coven's.
