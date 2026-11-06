# Notes: proving coven in Lean

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
- So the merge rules in the spec (§8 of `spec/coven.md`) were
  modelled in Lean 4 and proved to converge. The Lean project is
  `spec/proofs/merge/`.

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
  and a separate table of held rows (`coven_held`) all went away. Each was
  a place the proof needed extra rules to state.
- **Lean on losing.** Where a concurrent change can't be merged, it loses
  and is recorded in `coven_lost`, so the app can offer it back.

## Round four: everything else (October)

The merge proof worked, so the rule became: no new rule goes into the spec until Lean proves it. The
sync, storage, identity, files and key rules got their own models (`spec/proofs/{storelog,storage,io,
queue,...}`). Each bug below was a checked counterexample first, then a Rust fix with a test:

- **Log retention deleted writes a newer version would need.** A device that couldn't read a snapshot
  passed over it; a newer coven that can read it then needs writes already deleted.
- **A reload skipped a snapshot it couldn't load** and silently dropped one circle's part of a write.
- **A reload needing a refused write waited for an app update** even after a snapshot covering the gap
  arrived.
- **Fingerprints leaked.** Hashes of app data were keyed with the newest key a device held; a removed
  member holding that key could test what a write they can't read contains. The fix then stalled a
  checkpoint whenever a pass ran past five minutes; Lean caught that too.
- **The stale-copy check compared the wrong counter**, so a restored backup could keep sending instead of
  resetting.
- **A reset left an S3 access key live** with nothing recording that it needed revoking.
- **Resetting on an unreadable own key copy only lost edits**: Lean showed that object can only be storage
  damage, never another copy.
- **A pass counted its first phase complete** with an unreadable key copy, reusing the previous pass's
  catch-up.
- **Schema shapes that can't merge**: a foreign key on a generated column merges into a reference no write
  recorded; a local child with `ON UPDATE NO ACTION` makes a remote update fail on every retry forever.
  Both are now refused at setup.
- **A member re-add overwrote another member's access.**

Not every model found a bug: some proved a rule we were unsure of (staging plus a conditional copy has
exactly one winner, given an atomic copy) and some showed a known limit (a listing is not a lock, so two
setups racing can both create a store).

## Checking the code against the models

Proving the spec doesn't prove the Rust. So each model also compiles to a small runner, and Rust tests
feed the same generated histories to the real code and to the runner and compare results: merge
outcomes, retention decisions, which snapshot a reload picks, identity verdicts, file status. About forty
of these run in CI.

## Worked example: an upload stuck at a shared name (November)

As of 2026-11-06, partway through; what has landed and what hasn't is marked below. The storage post
tells the provider side of this.

**The model.** `UploadSession.lean` (`spec/proofs/storage/CovenStorage/`) models one upload going up in
parts through a provider session: the upload worker's runs, crashes, a provider that expires sessions,
and the retry of each failure. The upload's place is a parameter: either the device's own, where no
other device's upload goes, or shared, like a slot every device claims.

- The first version took "the provider begins a session" and "the device records it" as one step. So a
  session the provider began that the device never recorded couldn't happen in the model.
- The next version split them. The device's row records a *plan* naming where the session will be,
  and marks it before the request that may begin it. `begin` became its own step with a lost reply as a
  fault: the provider begins the session, the reply never arrives, and the session is a *stray*. A crash
  after the start leaves one too.
- The model proves those two steps are the only ones that leave a stray (`steps_stray_le`), and that
  every stray sits at the place of the marked plan the row still records (`stray_named`).

**What the model said.** At the device's own place, every session there is this upload's, so the next
attempt ends them all first (`open_sweeps`). At a shared place nothing tells a stray from another
device's session, and the model can only fail. `marked_shared_never` states it: a marked plan at a
shared place fails with the class that never retries. That theorem describes what the design does, and
stating it as a theorem put the stuck case in writing: one lost reply, or one crash at the wrong moment,
and the upload never goes out.

**Narrowing it.** Before deciding what to do, the model was made exact about when a plan is marked,
and that found two bugs in the code, both fixed:

- The mark lived only in memory, so after a restart every plan at a shared slot was taken as maybe
  started, and stuck, even one whose start had never been sent.
- The start can be refused as it goes out, after the plan was marked, when the publication gate closes
  or a provider cooldown begins in between. That refusal sent nothing, but left the plan marked. Now
  it unmarks the plan (`refusal_unmarks`).

What remained was the real case: a start that went out and whose reply was lost. No change to recovery
fixes that, because what the device needs, which upload at the slot is its own, isn't in storage. So
the fix moved where uploads go: on every S3 service a large upload goes to a name of the device's own
and is published by one copy that refuses a taken name. No unfinished upload ever sits at a shared
place.

**Three layers keep it gone.**

1. **The theorem.** In the new model a plan names a place of the device's own whatever the
   destination, and a theorem (`no_shared_strays` on the branch) says no stray is ever at a shared
   place. The proof is one line: the model's plan place is `.own` for every destination, so no step can
   put a stray anywhere else. The work moved into the model's shape. `open_sweeps` loses its "the
   place is the device's own" premise, and `marked_shared_never` goes away. *Committed on a coven
   branch, not landed.*
2. **The differential test.** `upload_sessions_match_the_storage_model` (coven-sync) runs the real
   upload worker over coven's in-memory provider and the model's runner on the same histories, once at
   an owned place and once at a shared one, and compares after each run: the queue row, the parts it
   records, the provider's sessions and their parts, the strays, the published object and the class of
   the failure. *Landed, with the shared case expecting the upload to fail for good. Its update, so the
   shared case expects the strays ended, comes with the code.*
3. **The type.** A profile's `S3CopyPublication` today is `Never` (complete in place), `Multipart`
   (copy objects that go in parts) or `Every` (copy every object). The plan drops completing in place
   from the type, so a new S3 profile can't choose it. *Planned, not started.*

Each layer covers what the others can't. The theorem is about the model. The differential test checks
the code against the model, on the histories it samples. And a new S3 profile is a value in a table: a
theorem about the model says nothing about a value someone adds later, but a type without the option
has no way to express it.

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
- Proofs cover the spec; the code is checked against the models by the
  differential tests above, which sample histories rather than prove.
