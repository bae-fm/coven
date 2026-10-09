# Notes: proving coven's merge in Lean

Raw material for a blog post later. What we did, how it works, and what it
found.

## Why a proof

- Coven's promise: every device that applied the same writes ends with the
  same database, whatever order the writes arrived in, and whatever order
  coven ran its rules in.
- The first plan was randomized tests: generate writes, shuffle the order,
  compare the results.
- That finds bugs, but it doesn't show there are none. A shuffle test only
  covers the orders and histories it happened to generate.
- So the merge rules in the spec (§8 of `coven-from-scratch.md`) were
  modelled in Lean 4 and proved to converge. The proof is Appendix B of the
  spec, and the Lean project is `spec/proofs/merge/`.

## How Lean proves something for every input

- Lean doesn't run inputs. It checks a written proof, step by step.
- The statement has variables, like algebra: for *any* list of writes `ws`
  that meets the assumptions, and *any* two orders of `ws` that respect
  causality, applying them gives the same state.
- The proof is written out, mostly as induction:
  - applying no writes gives the empty state;
  - if the first *n* writes give the state B4 defines from the set of
    writes alone, then applying write *n+1* still gives B4's state (the
    "step lemma");
  - B4's state depends only on the set of writes, not their order, so any
    two orders end the same.
- Lean checks that every step follows from the earlier ones by the rules
  of logic, down to a small trusted core. A missing case, or a step that
  doesn't actually follow, fails to compile.
- So a proof that builds holds for every input, infinitely many of them,
  not just the ones anyone thought to try.

## The assumptions

The whole proof rests on four facts about writes, each guaranteed elsewhere
in the spec:

1. timestamps are unique;
2. a write's timestamp is larger than that of every write it had read;
3. a change's generation was reached on the device that made it;
4. an insert is made at an even generation, an update or delete at an odd
   one.

## Why it finds bugs

- When the spec is wrong, the proof gets stuck: some step won't go
  through.
- Trying to fix the step turns up the concrete case where the spec really
  is wrong: two arrival orders of the same writes that end differently.
- Lean then checks that counterexample too, by running both orders through
  an executable reading of the spec's rules.
- So every bug below came with a concrete example, checked by machine.

## Round one: the first draft of the merge

Each is two arrival orders of the same writes that end differently.

- **Lost values depended on arrival order.**
  - Ana writes title "Groceries"; Ben, having read it, writes "Weekly
    groceries"; Carol, having read neither, writes "Shopping", the
    latest.
  - Ben's phone records one lost value; Carol's tablet, which got Ana's
    write before Ben's, records two.
  - The rule had no way to un-record "Groceries" once Ben's write, which
    had read it, arrived later.
  - Fix: a value is lost when some write replaced it and *no* write that
    replaced it had read it. That's a function of the set of writes, not
    their order.
- **Which delete a generation records.** Two concurrent deletes of note
  43, stamped 50 and 60. Applied 50 then 60, the record names 50; applied
  60 then 50, it names 60. Fix: record the smallest timestamp.
- **"Replaced by" after a re-add.** Only the latest generation change was
  kept, so a late edit was recorded as replaced by the re-add on one
  device and by the delete on another. Fix: keep one small record per
  generation, forever.
- **Cascade for a child the deleting device never had.**
  - Tags cascade from notes. Ana tags note 43 with tag 9; Ben moves tag 9
    to note 44; Carol, who never saw tag 9, deletes note 43.
  - Ben's phone: tag 9 is already on note 44 when the delete arrives, and
    stays.
  - Carol's tablet: tag 9 arrives pointing at a deleted note, gets
    deleted, and Ben's move then loses to that delete.
  - Fix: the cascade isn't stored as a delete. It's a *rule* that takes
    the row out of the app's table while the reason holds, and puts it
    back when it stops holding.
- **Unique and CHECK losers removed for good.** Same shape: storing the
  removal as a delete made the result depend on whether a later fixing
  write arrived before or after the conflict.
- **Unique plus cascade.**
  - Folder 70 holds note 45 "Groceries"; Ben adds note 46 "Groceries";
    Carol deletes folder 70.
  - Run cascade first: note 45 goes, note 46 has no rival and stays. Run
    the unique rule first: note 46 loses, then note 45 cascades, and
    neither stays.
  - This fails on a single device, depending only on rule order.
- **Ancestors** (rows kept alive while a shared row points at them): a
  stored "retract" of an ancestor applied or not depending on whether a
  concurrent new child had arrived yet.
- **Unique values across circles.** A device outside a circle can't see
  the circle's claim to a value, so two devices disagree on a store row.

## The general shape that fell out

- Instead of checking rules pair by pair, the proof uses one property:
  a rule is *monotone* if it keeps firing for a row when more rows are
  removed. "Your parent is gone" stays true as more rows go.
- For monotone rules over finitely many rows:
  - applying them always stops, since each step removes a row;
  - two steps from one state can always meet again, since each rule
    still fires after the other;
  - by Newman's lemma, every order ends in the same state, the smallest
    set of removed rows that leaves no rule firing.
- So any new rule can be added without re-checking every pair, as long as
  it is monotone.
- The unique rule isn't monotone: removing the winner stops it firing for
  the loser. So it runs exactly once, between two passes of the others.
  Lean proves those three steps have one result.

## Round two and three: audiences and wording

After the spec added audiences (store, circles, this device), the proof was
redone against the new text, and found:

- **A store row kept alive only by a circle's row.** A label worn only by
  a todo in Ana's circle: Ana's device shows it, and Dan's, outside the
  circle, takes it out, though both have it as a store row. Settled: each
  device shows such a row only while a row it can read keeps it.
- **Re-adding a row while it moves into a circle.** Ana moves note 1 into
  her circle while Ben, outside it, re-adds note 1 in the store. A device
  in the circle merged the two into one row; when Ana moved it back,
  devices outside skipped a generation and Ben's value vanished without a
  record. Fix: count generations per audience.
- **Chains of key changes could loop.** Concurrent renames "urgent" →
  "important" and "important" → "urgent" pointed at each other, so a
  reference following renames never settled. This one led to a
  simplification rather than a fix: a key change is now a delete plus an
  insert, and children elsewhere just see their parent deleted. No chains
  to follow.
- **When a unique loser comes back.** A loser coming back whenever "the
  winner left for some other reason" allows two different end states.
  The proven rule: it comes back only when the winner is deleted or taken
  out before unique values are judged.
- **The fingerprint check raised false alarms.** Devices compare hashes
  of their data to catch bugs; rows whose visibility depends on which
  circles a device reads made correct devices disagree. Fix: hash the rows
  as if those rules didn't exist, which Lean proves devices agree on.

## What the proof pushed the design toward

- **Derive, don't store.** Every removal (foreign keys, CHECK, unique,
  ancestors) became a function of the merged writes, recomputed, never
  written as a delete. Stored removals were the source of most round-one
  bugs.
- **Simpler is easier to prove, and the hard-to-prove parts were the
  over-designed ones.** Key-change following, "readings" for set null,
  and a separate table of held rows all went away. Each was
  a place the proof needed extra rules to state.
- **Lean on losing.** Where a concurrent change can't be merged, it loses
  and is recorded in `_coven_lost`, so the app can offer it back.

## Facts and figures

- Lean 4.34.1, no Mathlib. Every lemma, including Newman's lemma, is
  written in the project.
- A clean build takes a few seconds.
- No `sorry` or `admit` (Lean's "trust me" escapes).
- `#print axioms` shows only Lean's standard three (`propext`,
  `Classical.choice`, `Quot.sound`), and many theorems use none.

## What it doesn't cover

- The model treats a removal rule's inputs, such as which rows are
  present or which references are stale, as any function of the merged
  state. That coven's code computes them as the spec says is argued, not
  proved.
- Triggers, schema changes, files and resets aren't modelled.
- It proves the spec, not the implementation. The implementation still
  needs tests, and could be checked against the Lean model's executable
  step function.

## Modelling the whole system: devices, storage and time

The merge proof covers one question: given a set of writes, do devices agree?
Most of what went wrong later lived outside it: upload queues, breaking schema
changes, resets, snapshots and retention. Those are about *events in time* on
several devices at once, so they're modelled differently.

- **A state machine.** The state is everything that matters: each device's
  queue of committed writes, what it has applied and its schema version; each
  storage slot (empty, or holding which write); the store log.
- **Steps.** Every event that can happen is a rule for how the state changes:
  commit a write; start an upload, which may succeed, fail, or fail and still
  land later; download and apply; raise the schema; update the app and reload;
  reset.
- **Safety properties** are statements about every state reachable by any
  sequence of those steps, in any order: every device reaches the same verdict
  on each write; nothing applied built on a dropped write; nothing is applied
  twice. They're proved by induction: true at the start, and kept by every step.
- **Eventual properties**, like "every queued write settles", say something
  must happen. They need an assumption about the world: each pending event
  happens at some point (fairness). The assumption has to be stated exactly; see
  the last counterexample below.
- **Lean checks a proof; it doesn't search for bugs.** The counterexamples come
  from trying to write the proof: a step that won't keep the property is exactly
  the sequence of events that breaks it. Once the proof is finished, the property
  holds for every order of events, not only the ones anyone thought of.
- **Model versus code.** The proof is about the model, not the Rust. The
  differential test connects them: it generates random histories, runs each
  through the Rust implementation and through the Lean model's executable step
  function, and compares the results. The merge and the store log already have
  one; each new model gets one too.

### What the queue and schema-change model found before any code changed

A data-integrity audit had found by hand that a write whose upload failed just
before a breaking schema change could be lost on every device but kept on its
author's, and that the author's later writes then stalled everywhere. Fixes were
designed in conversation and handed to the model to check. It rejected four
designs in a row, each with a concrete history:

- **A converted write that built on a peer's dropped write.** Ana inserts row r
  just after Carol's breaking-change snapshot S is taken, so S lacks it. Ben,
  still on the old app, edits r; his edit waits for his update. After the change,
  Ana's insert is dropped everywhere, but Ben's converted edit still points at r,
  and every device refuses it. This happens in today's design with no failed
  upload at all. Fix: one rule every device applies the same way, from what each
  write read: a write that read a dropped write is dropped too.
- **A snapshot rule that deadlocked.** "Write snapshots only with nothing
  waiting to upload" was added so no unsent write could leak through a snapshot.
  But the device making a breaking change queues its own migration write, which
  waits for the raise entry, which needs a snapshot. Fix: drop the rule; it was
  only needed for a renumbering design that had already been abandoned.
- **Coverage counted as storage.** "If the raise snapshot covers the unsettled
  write, treat it as stored" fails when the snapshot is the device's own, which
  can include its own unsent writes. The slot stays empty and everyone waits on
  it forever. Fix: an unsettled write is always settled by resending its exact
  original bytes; coverage only decides whether it's then kept or dropped.
- **Fairness stated too weakly.** "Every upload attempt eventually resolves"
  allows every retry to fail forever, so nothing ever settles. Settlement needs
  the assumption that storage eventually accepts a retried write. That is an
  assumption about the provider, and the model now states it.

Two more had been caught by hand while designing, and are the kind the model
checks: a copy of a write must not be made when the snapshot already includes
the original, or it applies twice; and an upload that seemed to fail can land
after a check found its slot empty, so "check, then convert in place" is a race.
