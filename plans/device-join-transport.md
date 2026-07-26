# Storage-mediated device-join transport

## Status

Planned. Coven's device join is a three-role handshake (Owner, Provider
Administrator, Joiner) whose nine happy-path artifacts
(`DeviceJoinAction::Transfer*`) coven hands to the host to deliver. Today
every host must build its own delivery. This adds the default transport to
coven itself — artifacts carried as sealed create-once objects in the
store's cloud home — while keeping the raw `DeviceJoinAction` surface for
hosts that want a different channel (QR-only, local network, relay).

The offer is NOT carried by this transport: it travels out of band as the
host's join code (bae: a QR the joiner scans). The offer bundle is what
bootstraps the transport — it names the slot namespace and carries the
seal key.

## Ground truth to build against

- Role journals and actions: `coven-core/src/sync/store/device_join/journal.rs`
  (`OwnerJoinProgress`, `ProviderAdminJoinProgress`, `JoinerJoinProgress`,
  `DeviceJoinAction`). The transport must not change any of these — it is
  a delivery layer under the existing state machines.
- Artifact types: `device_join/exchange.rs`, `store_commit/device_join.rs`.
  All artifacts are already `Serialize + Deserialize` with
  `deny_unknown_fields`, signed, and hash-bound to predecessors — the
  transport treats them as opaque payloads and adds no interpretation.
- In-process orchestration to mirror: `coven/src/sync/join.rs` and
  `join_tests.rs` (the tests pass artifacts as variables; the transport
  replaces exactly that passing).
- Create-once machinery: `create_protocol_object` / `ObjectSlot` /
  exact-read (`storage/` + `sync/store_objects.rs`). Compose these; do not
  invent a second slot primitive.

## Design

### 1. Offer bundle (the out-of-band kickoff)

```rust
pub struct DeviceJoinOfferBundle {
    pub version: u32,
    pub offer: DeviceJoinOffer,
    pub transport: DeviceJoinTransportParams,
}

pub struct DeviceJoinTransportParams {
    pub attempt_namespace: String, // derived from the offer's attempt id
    pub slots: BTreeMap<DeviceJoinTransportKind, ObjectSlot>,
    pub seal_key: MasterKeyring,
}
```

Ratified: the bundle carries the attempt's slots, allocated by the owner at
bundle-minting time, rather than each side deriving them from the namespace —
a provider may answer an allocation with an opaque locator that no reader can
reconstruct from a logical key, which is the same reason the protocol reserves
its attempt and outcome slots in the artifact that precedes them; the cost is
that minting a bundle makes one allocation round trip per artifact kind.

Serializable to bytes (host encodes as QR / code / link however it wants).
The seal key is minted fresh per offer by the Owner at `begin_device_join`
time. Reuse the existing symmetric sealing primitive from the codebase
(the same family the routing/session sealing uses) — do not add a new
cipher surface.

### 2. Slot layout

One namespace per attempt under the store home:

```
store-v1/device-join-transport/{attempt_id}/{artifact-kind}
```

Eight artifact kinds (the post-offer happy path plus terminal/cleanup
artifacts as distinct kinds — enumerate from `DeviceJoinAction`:
access-request, admission-approval, registration-request,
provisional-bootstrap, provider-ready, readiness, admission-completion,
activation; plus abandonment, cancellation, the two terminals, cleanup
receipt/activation for the unwind paths). Each slot:

- **create-once** via the existing protocol-object machinery — a second
  write to the same slot is the existing slot-conflict error, surfaced,
  never retried blindly;
- sealed with the bundle's seal key before write; unsealed + then normal
  artifact verification (signatures, hash chaining) after read — the seal
  hides artifacts from the storage provider, it is not part of the trust
  story (verification never trusts the seal);
- written by exactly one role (each artifact kind has one producer in the
  protocol — encode the producer role in the kind table and assert it).

### 3. Transport API

```rust
pub struct DeviceJoinTransport { /* home handle + params */ }

impl DeviceJoinTransport {
    pub fn publish(&self, artifact: &DeviceJoinAction) -> ...;   // seal + create-once write
    pub async fn await_artifact(&self, kind: ..., poll: Duration, deadline: Duration) -> ...;
}
```

- `publish` maps a `Transfer*` action to its slot; publishing a kind whose
  slot is already occupied with identical bytes is idempotent success
  (crash-resume republishes); different bytes is a typed error.
  Ratified: sameness is decided on the unsealed artifact, not on the stored
  ciphertext, because the seal draws a fresh nonce per call and so a
  republished artifact never reproduces its first write's bytes; the first
  write's bytes stay in the slot.
- `await_artifact` polls at the caller's interval with a hard deadline; a
  deadline expiry is a typed timeout error naming the counterpart role
  that never responded (the host renders "the owner's app must be open").

### 4. Role drivers

The drivers connect the existing journals to the transport — thin loops,
no protocol logic:

- **Joiner**: `join_via_transport(bundle, home, ...) -> Config` — the
  one-call surface. Internally: run the existing joiner steps; after each
  step, publish the produced artifact; before each step, await the
  counterpart artifact. Resumes from the joiner journal at any crash.
- **Owner + Provider Administrator**: `drive_device_join(home, session)`
  — one driver advancing both role journals as artifacts arrive (the
  common co-located case; separated parties each run it with their own
  role's keys and it simply finds only its role's work). Started by the
  host while the app runs; returns when the join reaches a terminal
  state. Approval policy: the driver takes a policy argument —
  `AutoApprove` (bounded to attempts this device issued) or a host
  callback for an explicit prompt. AutoApprove is the default the host
  opts into, not a hidden behavior.

### 5. Cleanup

Transport slots are deleted at the same points the protocol already
deletes its probe/attempt objects: after the joiner's
`complete_device_join`, and during the cancellation unwind
(`ProviderAdminJoinClosure` / joiner cleanup dispositions). No sweep, no
orphan-collector; an unfinished attempt's slots are removed by that
attempt's cancellation path or not at all.

Ratified: the whole namespace is deleted at once by the side that reads
last, rather than each role deleting what it wrote. That side is the
joiner on all three paths — its completed join, its accepted abandonment,
and its accepted cleanup activation are each the point at which every
artifact has provably been consumed. A role deleting its own writes as it
went, or the owner deleting the namespace at the end of the cancellation
unwind, would race the joiner's read of the last artifact the owner
published; there is no artifact by which the owner could learn that read
had happened.

## Correctness cases

- **Crash anywhere**: journals already resume; the transport adds only
  idempotent republish (same bytes → success) and re-await. No new
  durable state beyond the slots themselves.
- **Two joiners race one offer**: the attempt slot (existing) decides;
  the loser's transport writes are cleaned by its cancellation path.
- **Tampered artifact in storage**: unseal fails, or unsealed bytes fail
  the existing signature/hash verification — both surface typed; the
  transport never retries a failed verification.
- **Wrong-attempt replay**: artifacts bind attempt ids already; a slot
  holding an artifact for a different attempt is a verification failure,
  not a transport concern.
- **Provider can't see plaintext**: every slot value sealed; slot names
  reveal only that a join is in progress (same information the existing
  attempt objects already reveal).

## Tests

Mirror `join_tests.rs` with the transport substituted for in-process
passing, over the in-memory cloud home:

1. Full happy-path join, joiner + owner drivers only (no manual artifact
   passing), ending in a verified member Config. Cross-principal (probe
   exercised) and same-principal variants.
2. Crash/resume at every artifact boundary on both sides (kill after
   publish, before await; after await, before journal advance) — resume
   completes; slots hold identical bytes (assert exact).
3. Duplicate publish: same bytes → idempotent; different bytes → typed
   error.
4. Timeout: no counterpart → typed timeout naming the absent role.
5. Sabotage: tampered slot bytes → unseal/verification failure surfaces;
   join does not advance.
6. Cancellation mid-join → transport slots for the attempt are gone after
   the unwind completes (assert absence).
7. Two concurrent attempts in one store → namespaced slots do not
   interfere.
8. Owner abandons an attempt the joining device is waiting on → the
   joiner reads the abandonment in place of the artifact it wanted and
   converges on the same terminal; slots gone.
9. Cancellation unwind driven one side at a time, each run dying at the
   artifact it waits for → converges on an activated cleanup; the owner
   leaves the cleanup activation readable for the joiner that has not
   consumed it yet.
10. Approval policy: `AutoApproveSelfIssued` refuses an attempt this
    device has no owner journal for; `Ask` is consulted and its refusal
    stops the join before any approval is published.

Not covered: a device holding only *one* of the two admitting roles. The
two admin-internal boundaries (provisional bootstrap, admission
completion) do travel through the transport and resume across driver
restarts in the co-located case — case 2 asserts both slots hold their
artifact — but the `roles.owner` / `roles.provider_administrator` guards
and the `!roles.owner` activation wait have no test. Building one needs a
store whose provider administrator is a device other than the founder,
which nothing in the repo constructs today:
`MembershipChain::signed_provider_admin_change_in_stream` is `pub` with
zero callers, and the only `ProviderAdminChange::Set` uses are fabricated
records in `provider.rs`'s own unit tests.

## Out of scope

- The offer's out-of-band encoding (QR rendering is the host's).
- Changing any journal, artifact, or verification — delivery only.
- Push/notification transports; polling cadence tuning beyond the
  caller-supplied interval.
