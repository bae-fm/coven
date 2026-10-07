# Store-log transport and keys

One commit implements the store-log storage step and its publication boundary.
`StoreLogSync` retains storage, database, member custody, store-key custody and
clock capabilities. Its two calls run a store-log step or make and publish an
entry; it starts no tasks, timers, status channel or sync loop.

Entries are authenticated at their storage paths, decoded with the format
codec, checked for causal closure and increasing timestamps, and applied
through replay and the database's atomic entry/result boundary (§§9–10).
Missing prerequisites and keys wait in storage; damaged objects retain their
paths and typed causes. Dropped entries are reported to their author. Applied
member/device removal stops the owner, as does losing the setup race (§4).

The database fixes an entry's number, timestamp, plaintext and sealed bytes,
along with its prerequisite sealed keys, before storage is called (§§6, 18).
It retires that queue in the transaction applying the published entry. Keys
publish before entries, occupied paths count as stored, and retries reuse the
recorded bytes. Writes and entries share their latest timestamp (§7.2).

Key distribution uses the author's applied membership view. Removals exclude
the removed member, circle keys reach only the remaining circle members, and
an outside admin retains no generated circle secret. Joins receive every held
historical store key; circle additions receive their historical circle keys.
Kept no-op entries' keys are acquired too, and old keys remain in custody
(§§11–14, 20.11).

## Decisions and specification edits

The encrypted creation timestamp could not decide a race between stores that
do not share keys. The store-log prefix now carries signed creation identity:
store id, timestamp and author. Its signature binds the path and ciphertext;
opening also verifies equality with the encrypted fields. §4 and Appendix D9
specify this, and the independent sealed fixture was regenerated. Other
store-log entries have an explicit absent-origin tag. Format 1 has one current
layout, without an older reader.

§§8–9 now describe `coven_store_log_uploads` and its sealed-key child table.
Only one entry may await publication; it must publish before authoring the
next, avoiding a second locally applied state for unpublished entries. The
queue is an optional entry in Rust and has a SQLite uniqueness constraint. Key
bytes are generated and sealed during reservation; plaintext key material is
never written to these tables. Local calls reject changes already dropped by
replay, retaining `DropReason`; remote entries still apply with dropped marks.
§20.5 records the concrete error variants and the report fields this step
fills. The ownership policy registers `StoreLogSync` as a root owner.

Only a newer unsupported envelope means `UpdateRequired`; version zero is
damaged. Local writes must follow fixed entries even if the wall clock moves
backward. Both distinctions have regression tests observed failing before
their fixes. A changed member custody cannot change an existing device's
entry author: local authoring rejects that mismatch before reserving bytes,
matching the downloaded-entry check. Its regression also failed before the fix.

## Specification issues

D6 includes provider-access fields that the canonical plaintext codec does not
carry. This work uses that codec and enforces D6's entry, path, identity,
ordering and membership rules. Provider access operations are outside this
store-log/key owner.

The stated key rules leave a liveness gap after a losing rotation: a member
excluded from that rotation can remain a member in the final replay but lack
the key sealing that author's subsequent fixed entries. No rule redistributes
that losing key to them. The implementation preserves the specified recipient
sets and waits for missing copies; it does not silently grant additional
access. This follows from the fixed-byte and author-view recipient rules; a
full cross-rotation continuation was not exercised in the integration tests.

## Verification

The memory-storage integration tests cover two and three devices, every
permutation of the concurrent uploads in §9's examples, causal waits, each
object check, missing and damaged sealed keys, restart with fixed bytes, lost
publication replies, key-before-entry publication, rotations and historical-key
joins, no-op keys, outside-circle admins, dropped reports, future timestamps,
new restored-device numbering, removal and setup races. Database tests cover
failed reservation and transactional rollback of queue retirement.

The §9 history permutations exercise authenticated transport and replay using
a held historical store key; the owner integration tests separately exercise
current-key selection and actual key distribution. Restart tests close and
reopen the real database after injected upload failure; they do not kill the
process between individual SQLite instructions. No live cloud credentials,
provider access revocation or cross-platform runtime was exercised.

`next/scripts/check.sh` passed in full before the original local commit and
again after the rebase onto `3276ee9c`: formatting,
Clippy, the ownership checker, documentation, production builds, both feature
configurations of the tests, both Lean proofs and axiom audits, both Rust/Lean
differential tests, and the 2,000-entry release replay test. The changed Rust
sources are at most 1,000 lines, and the diff has no whitespace errors.

The rebase onto `3276ee9c` preserves upstream provider metadata and multipart
transfers. The
owner takes entry paths from the listing metadata; its tests exercise lost
multipart completion replies through the provider's existing fault injection.
The creation envelope fixture was regenerated from upstream's kind-4 plaintext.
Provider-access fields remain outside this owner's required capabilities; no
fields were added solely for future provider removal work.

Part 3 was not present in the rebase target or available as a local sync branch.
Integration with its device-log and posted-position owner was not checked.
