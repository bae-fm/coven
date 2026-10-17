# One store sequence

The Lean package in [`sequence/`](sequence/) checks the proposed replacement of
per-device store logs and write logs with one numbered sequence per store. The
normative spec does not describe the sequence yet; this package checks the
design before the spec changes. It uses Lean 4.34.1 and its standard library,
without additional axioms or unfinished proofs. All named results are audited by
`CovenSequence/Axioms.lean`, and `scripts/check.sh` builds the package and runs
the audit with the other proofs.

Slot `<store>/seq/<n>` is claimed create-once. It holds an admin entry, one
device's batch of data writes for a sync pass, a key copy, a snapshot
announcement ("the store as of slot `c` is at path `p`"), or a schema raise,
which announces its snapshot the same way. Every device applies slots in strict
order.

Three results are checked counterexamples, and two of them force rules the
design did not state:

- Snapshot coverage alone (C6) does not remove the raise retention hold. A
  snapshot announced after a raise must also cover that raise
  ([Snapshots and deletion](#snapshots-and-deletion)).
- Deleted slot paths are free again for create-once. A claimant and a reader
  must each read the floor record after the slots they read
  ([Claiming a slot](#claiming-a-slot)).
- A restart that does not resolve its recorded claim lands an operation twice
  ([Operations](#operations)).

## The model

[`Model`](sequence/CovenSequence/Model.lean) keeps every type abstract: devices,
members, unique keys, paths, batches, rows, and admin operations other than
membership (roles, renames, access, circles), whose state is `aux`. The rules
are abstract too: authority, the effect of other operations, the merge, the
converter of old-version batches, and the database part of a migration.

A device's state has two parts:

- **The ledger** holds everything slot order alone decides. That is members,
  devices and their removal, member key generations, `aux`, the first slot that
  claimed each unique value or owner row, key copies, announced snapshots, the
  schema version, and refused slots.
- **The rows** come from the abstract merge. A batch at an older version is
  converted to the store's version. A valid raise at slot `r` covering `c` loads
  the raiser's migrated rows as of `c`. It then replays the counted batches of
  slots `c+1` to `r-1`, converted to the new version.

A slot invalid at its position is refused. It is consumed, and only its refusal
is recorded. Removing a member removes every device registered to it, so
re-adding the member does not revive them.

A batch counts when its device is registered, not removed, and its member is a
member. It must also have been made under that member's current key generation,
at a version the store has reached. A raise must cover every announced
snapshot (C6) and the last raise, and must raise the version. A snapshot must
cover less than its own slot, and under the version rule it must cover the last
raise.

## Claiming a slot

[`Claim`](sequence/CovenSequence/Claim.lean) models the provider's slots as a
store that only fills empty slots. "Created" is answered only when the slot
holds the claimant's bytes. "Already exists" and a lost reply can come at any
time, including to the claimant that won. The protocol: "created" means won.
Otherwise read the slot: own bytes mean won, other bytes mean lost, and an empty
slot means send the same bytes again.

- `conclude_correct`: a claimant that concludes `won` holds the slot forever.
  One that concludes `lost` sees other bytes there forever, so its own create
  can never land.
- `occupant_claimed`: every occupied slot holds the bytes of a claimant that
  created it.
- `exactly_one_claimant`: with bytes unique per attempt, every occupied slot has
  one claimant. That claimant concludes `won` whenever it concludes anything,
  and every other claimant concludes `lost`.
- `no_compare_lands_twice` is a checked counterexample to the protocol without
  the compare. The winner, answered "already exists", concludes `lost`, claims
  the next slot with the same batch, and its batch occupies two slots.

Deleting covered slots frees their paths, and a create-once request to a freed
path succeeds. `Hist` adds deletion and the floor record. The floor is raised
before slots below it are deleted and is never lowered. A ghost record keeps
each slot's first bytes, which are its genuine content.

- `stale_create_wins_deleted_slot` is a checked counterexample. Slot 1 is
  claimed, covered, the floor raised and slot 1 deleted. A stale create of slot
  1 is then stored and truthfully answered "created". It might have been sent
  before its claimant saw the claim, or after the device slept past the grace
  period. The claimant concludes it won a slot no device at the floor reads. A
  reader below the floor reads the stale bytes where the genuine content was
  different.
- `floor_check_sound`: bytes read at a slot are its genuine first bytes, and
  stay so, when a later floor read has not passed the slot. So a claimant reads
  the floor after its claim lands, and a reader reads it after the slots it
  read and before committing them. One floor read at the end of a pass
  validates every slot the pass read. Keeping claim records forever (Q5's
  option (i)) is the alternative: the paths then never free.

## Convergence

[`Converge`](sequence/CovenSequence/Converge.lean):

- `prefix_determines_state`: two sequences that agree on slots `1..k` give the
  same ledger and rows after `k`. A device's state is a function of the prefix
  it applied.
- `valid_prefix`: whether slot `n` is valid depends only on slots `1..n-1`.
  What an operation validated before claiming is what every device checks when
  it applies the slot.
- `readers_agree` (in `Reader`): two readers at a prefix state and the same
  position hold the same state. This holds on different storages, for example
  before and after a deletion.

## The removal cut-off

[`Cutoff`](sequence/CovenSequence/Cutoff.lean):

- `cutoff`: a batch at slot `k+1` by a device cut off at an earlier applied slot
  `j` is refused. The cut-off is the device's removal, the removal of the
  member it was registered to, or the replacement of that member's key at or
  above the generation the batch was made under. The ledger only records the
  refusal: no unique value or owner row is taken. The rows are unchanged. Every
  device agrees by `prefix_determines_state`.

The model reads Q4's "after its member's key replacement" as "made under a
replaced key". A batch the member's other devices make under the new key
counts. The literal reading would refuse every later batch of a member whose
key was ever replaced.

## Unique values and owner rows

[`Unique`](sequence/CovenSequence/Unique.lean):

- `first_slot_wins`: starting with no owners, a key's owner after `k` slots is
  `i` exactly when slot `i ≤ k` is the first counted batch claiming it. It has
  none exactly when no counted batch up to `k` claims it. Owner creation uses
  the same rule.
- `owner_stays`: an owner, once chosen, never changes. A long-offline device's
  late claim loses whatever its timestamp.

## Snapshots and deletion

[`Reader`](sequence/CovenSequence/Reader.lean) models a device following
storage. Storage holds retained slots, the floor record (log start `n`,
snapshot path `p` covering `c`, with `n ≤ c + 1`) and snapshot bodies. A reader
steps in three ways:

- it applies the next slot;
- it applies a valid raise by loading the raise body and replaying the slots
  after the raise's coverage, read from storage;
- it reloads from the floor's snapshot when that covers more than it applied.

`Good` is what retention keeps true. Retained slots are genuine. Every slot at
or above the floor is retained. The floor's snapshot was validly announced, and
its body is the state it covers. A valid raise's body, when present, is the
raiser's migrated rows, and it stays present while the floor has not covered
the raise.

- `read_sound`, `reads_sound`: every reader step from a prefix state reaches a
  prefix state, whatever was deleted.
- `read_progress`, `reaches_head`: under the version rule, a reader at a prefix
  state below the head can step forward, and some run reaches the head state.
- `joiner_reaches_head`: a joiner that loads the floor's snapshot and follows
  storage reaches the state of a device that applied every slot.
- `retain_good`: retention keeps storage good. Retention deletes a slot below
  the floor, raises the floor (never lowering its coverage) to a validly
  announced snapshot whose body is stored, or deletes a body that is not the
  floor's and names no raise above the floor's coverage.
- `claim_good`: claiming the next slot keeps storage good, given that a raise's
  body was stored before its claim.
- `deletion_keeps_reachable`: after any retention, every reader reaches only
  prefix states and can still reach the head state. Deleting covered slots never
  changes any device's reachable state.

**The raise hold.** The proof of `read_progress` needs every valid raise above
the floor's coverage to cover the floor's snapshot. C6 gives this for a snapshot
announced before the raise. A snapshot announced after the raise can still
cover a slot before it. A device can do this: it captures at slot 2, a raise
lands at slot 3, and the device's refused claim re-validates and claims slot 4
with its old snapshot.

- `ReaderExamples.c6_alone_strands_reader` is a checked counterexample under
  C6 alone. Storage starts good. The floor is raised to that snapshot and slots
  1 and 2 are deleted as covered, and storage stays good. A joiner loads the
  floor's snapshot and is stuck below the head: slot 3 is a valid raise whose
  replay needs the deleted slot 2.

The version rule closes this: a snapshot announced at slot `a` must cover the
last raise before `a`, so it is at the store's version. Under that rule and C6,
the hold is unnecessary (`deletion_keeps_reachable`), and the same announcement
is refused. Raise bodies can be deleted once the floor covers the raise.

## Operations

[`Operation`](sequence/CovenSequence/Operation.lean) models one admin operation
as a journal entry. It records each pre-claim artifact before creating it. It
records the slot it claims before sending the claim, then resolves that claim
before doing anything else. Its bytes name the operation and the slot. A crash
loses nothing the steps use: every step reads the journal and storage, and a
restart takes the same steps. Sent claim requests survive crashes and can land
at any later time. Other devices claim slots at any time. Validation
(`allowed`) reads only slots `1..n-1`.

- `step_inv`, `steps_inv`: every step keeps the invariant.
- `operation_outcome`: over any interleaving, the operation's bytes occupy at
  most one slot. A landed operation's bytes are at its slot; the slots before
  it were filled when it validated, and validation held against them. An
  undone operation has no bytes in the sequence, now or later, and none of its
  recorded artifacts remains in storage.
- `forgetful_lands_twice` is a checked counterexample. A restart that does not
  resolve its recorded claim validates again after the first claim landed. It
  then claims the next slot and lands there too.

## Assumptions and what is not modelled

- Artifact creates are completed provider calls. A create request that lands
  after its undo deleted the artifact would leave it behind. The undo must
  outlast every artifact request it sent, or the model's claim does not hold.
- Snapshot and raise bodies are honest: each is the announcer's state as of its
  coverage, or the raiser's migrated rows. Retained slots are genuine because
  of the floor check above; `Reader` assumes this rather than composing with
  `Claim`.
- A batch at a version above the store's is refused. A device whose app cannot
  apply a slot stops before it; that stop is not modelled.
- Not modelled:
  - Drive's head file and its compare-and-swap;
  - snapshots per audience (circles): the floor names one snapshot;
  - the grace period, which is a cost setting and not a correctness rule;
  - unannounced snapshot bodies and orphaned batch objects;
  - releasing a unique value;
  - joiners reading slots before they hold the store key;
  - the merge's internals, which the merge package covers.
