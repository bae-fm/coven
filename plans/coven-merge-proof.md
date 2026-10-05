# Appendix: convergence of coven's merge

This appendix checks the merge rules of `coven-from-scratch.md` §8 against
the claim that every device that applied the same writes ends with the same
database (§3, §8). It has two parts:

- a proof, checked by Lean 4, that the core of the merge converges: cells,
  deletes and generations, and `coven_lost`, with three corrections to the
  spec;
- counterexamples, also checked by Lean, showing that the rules that remove
  rows after a merge — foreign key actions for rows the deleting device never
  had, unique values, CHECK constraints, and the ancestor rule for places —
  do not converge as written; and the general structure that makes them
  converge, with its proof.

The Lean development is in `proofs/merge/`. A11 lists exactly what it
proves and what is argued here only in prose.

`§` refers to sections of `coven-from-scratch.md`; `A` to sections of this
appendix.

## A1. Results

- **Core: proven.** Cells, generations and `coven_lost` converge: any two
  orders that respect causality give the same state. The state is a function
  of the set of writes alone, and applying a snapshot then the writes after
  it gives the same state as applying everything.
- **Three corrections the core needs**, each shown necessary by a checked
  counterexample (A8):
  - `coven_lost`: the per-arrival recording rule of §8.1–8.2 diverges on the
    spec's own example. The fix: a value is lost when some write replaced it
    and no write that replaced it had read it.
  - `coven_rows`: "the write that last changed its generation" depends on
    arrival order for concurrent deletes or re-adds of one generation. The
    fix: of the writes that moved the row to a generation, the one with the
    smallest timestamp.
  - `coven_rows` keeps only the latest generation change, so a late change
    made at an older generation can't name the delete it lost to, which
    §8.3's example requires. The fix: keep the write for every generation
    change.
- **Rules that remove rows after a merge: not convergent as written.** Six
  counterexamples checked by Lean and one argued in prose (A8.4–A8.10). Two
  causes:
  - a removal is stored like a delete and kept for good, though whether it
    happens depends on which concurrent writes have arrived;
  - the unique rule is judged among rows still present, so the order in which
    rules run changes the result.
- **General structure: proven.** If every removal rule fires on a condition
  that stays true when more rows are removed, the rules have exactly one end
  result in any order, and that result is the smallest set of removed rows
  closed under the rules. No pairwise checking of rules is needed. The unique
  rule fails this condition; judging it once, among the rows that survive the
  other rules, restores it.
- **Places: the ancestor retraction rule diverges** (A8.10). An ancestor's
  presence should be derived from its present shared children, which is a
  rule of the convergent kind above.

## A2. Terms

- **Write**: one transaction, with its device, number, timestamp, had-read
  positions and row changes (§5).
- **Row change**: one row's insert, update or delete within a write. A write
  makes at most one change per row.
- **Had read**: write `w` had read write `a` when `w`'s device had applied
  `a` before making `w` (§7.1). A device's own earlier writes count.
- **Concurrent**: neither of two writes had read the other.
- **Causal order**: an order of applying writes in which each write comes
  after every write it had read. Every device applies writes in some causal
  order (§7.1).
- **Applied set**: the set of writes a device has applied. It is *closed*: it
  holds every write any of its writes had read.
- **Generation**: per row, as in §8.3. Odd while the row exists, even while
  it is deleted, 0 before it is created.
- **Incarnation**: one lifetime of a row between generation changes. An insert
  made at generation `g` creates incarnation `g + 1`; an update or delete
  made at generation `g` acts on incarnation `g`.
- **Setter**: a write that sets a cell. Its value is the cell's value in that
  write. Two setters of one cell in one incarnation compete by timestamp.
- **Replacer** of a value: a write that sets the same cell in the same
  incarnation with a larger timestamp, or deletes that incarnation.
- **Lost value**: a value recorded in `coven_lost`.
- **Post-rule**: a rule that removes a row, or changes a reference, because
  of other rows: foreign key actions, unique values, CHECK constraints, the
  ancestor rule.
- **Removed row**: a row a post-rule takes out of the app's table.

## A3. Model and assumptions

- Writes are abstract identifiers with three pieces of metadata:
  - `ts w`: the timestamp, a number;
  - `past w a`: whether `w` had read `a`;
  - `chg w r`: `w`'s change to row `r`, if any: its kind, the generation it
    was made at, and which columns it sets.
- A cell's value is identified with the write that set it, since a write sets
  one value per cell. The app's value is that write's value.
- Assumptions, all guaranteed by §7.2 and §8.3:
  - **Assumption 1**: timestamps are unique;
  - **Assumption 2**: a write's timestamp is larger than that of every write
    it had read;
  - **Assumption 3**: a change's generation was reached on the authoring
    device: it is 0, or some write the author had read moved the row to it;
  - **Assumption 4**: an insert is made at an even generation, an update or a
    delete at an odd one.
- Assumption 3 is weaker than "the author's current generation", so the
  theorem covers any write whose generation the author had seen.
- Rows are independent in the core: a row's state depends only on the
  changes to that row. Foreign keys, unique values, CHECK constraints and
  places are post-rules, treated in A9.
- What a device stores, the state:
  - `gen r`: row `r`'s generation, as in `coven_rows`;
  - `genWrite r n`: the write recorded for generation `n` of row `r`. The
    spec keeps this only for the current generation; the model keeps it for
    every generation (A8.3);
  - `cell r c`: the write whose value cell `c` of row `r` holds, as in
    `coven_cells`; empty while the row is deleted;
  - `lost r c a`: the `coven_lost` row for the value write `a` set in cell
    `c` of row `r`: the incarnation it was set in, and the write recorded as
    replacing it. The spec's `coven_lost` has no incarnation column; it is
    needed to tell which lost rows a delete affects.

## A4. The theorem

- **Convergence.** Let `L₁` and `L₂` be two causal orders of the same set of
  writes. Applying `L₁` to the empty database and applying `L₂` to the
  empty database give the same state: the same generations, generation
  records, cells and `coven_lost` rows. Lean: `merge_converges`.
- **The state is a function of the set.** For every causal order `L`, the
  state after applying `L` is the declarative result for the set of writes in
  `L` (A5), and there is only one such state. Lean: `run_isSpec`,
  `isSpec_unique`.
- **Snapshots** (§15). Device A applies a causal order `L₀` and writes a
  snapshot. Device B loads it and applies `L₁`, in an order where the
  snapshot's writes count as applied. Device C applies `L₂` from scratch. If
  B and C applied the same set, they hold the same state. Lean:
  `snapshot_converges`.
- **Timestamp order.** A list of writes sorted by timestamp, holding
  everything its writes had read, is a causal order. So every causal order
  gives the result of applying the writes in timestamp order. Lean:
  `causalOrder_of_ts_sorted`.

## A5. The result, as a function of the set of writes

For a closed set `S` of writes, each part of the state is defined from `S`
alone:

- **Generation** of row `r`: the largest generation any write in `S` moved
  `r` to, by an insert or a delete; 0 if none. Every generation below it was
  reached by some write in `S`.
- **Generation record** for generation `n` of row `r`: of the writes in `S`
  that moved `r` to `n`, the one with the smallest timestamp. This is the
  write that does it first when the writes are applied in timestamp order.
- **Cell** `c` of row `r`: of the writes in `S` that set `c` in the current
  incarnation, the one with the largest timestamp. Nothing while the row is
  deleted.
- **Lost**: the value write `a` set in cell `c`, incarnation `k`, is lost
  exactly when:
  - some write in `S` replaced it; and
  - no write in `S` that replaced it had read `a`.
- **Replaced by**, for a lost value of incarnation `k`:
  - if `k` was deleted: the earliest delete of `k`, which is the generation
    record for `k + 1`;
  - otherwise: the cell's current value's write.

Example, the §8.1 writes on note 42's title:

```
Ana's write 1   "Grocery list"       12:00:00.000 #0
Ana's write 4   "Groceries"          13:01:00.000 #0   had read Ana 1
Ben's write 9   "Weekly groceries"   13:01:00.000 #1   had read Ana 1, Ana 4
Carol's write 2 "Shopping"           14:00:00.000 #0   had read Ana 1
```

- Cell: Carol's write 2, the largest timestamp.
- Ana's "Grocery list": replaced by Ana 4, Ben 9 and Carol 2, all of which
  had read it. Not lost.
- Ana's "Groceries": replaced by Ben 9 and Carol 2. Ben 9 had read it. Not
  lost.
- Ben's "Weekly groceries": replaced by Carol 2 only, which hadn't read it.
  Lost, replaced by Carol's write 2.
- This is the single row §8.2 shows. Lean runs the proven step on both of
  §8.1's orders and gets it: `example_8_1`.

Why "no replacer had read it":

- If a write that replaced the value had read it, that write's device had
  the value, either in the cell or in `coven_lost`, where the app could show
  it. So the value wasn't silently dropped.
- §8.3's rule for two concurrent deletes, "a value is lost only if neither
  had read it", is this rule for deletes. The rule here extends it to cells.
- It is the rule a device can keep without storing old values. A value's
  status changes in only two ways as writes arrive:
  - a value nobody replaced, which is always the cell's current value,
    becomes lost when a write replaces it without having read it;
  - a lost value stops being lost when a write replaces it having read it.
  A value replaced and not lost stays not lost, since the replacer that had
  read it stays in the set. So a device needs only the current cells and
  `coven_lost`.

## A6. The step: applying one write

A device applies an arriving write `w` to each row `r` it changes. Let `G`
be `r`'s generation on the device and `g` the generation `w`'s change was
made at.

- **Generation.** An insert or delete with `g = G` moves the row to `G + 1`.
  Any other change leaves `G` alone.
- **Generation record.** An insert or delete made at `g` competes for the
  record of `g + 1`: the smaller timestamp stays.
- **Cells.**
  - A delete with `g = G` empties the row's cells.
  - An insert with `g = G` starts a new incarnation with its own values.
  - An update at `G`, or an insert at `G - 1` concurrent with the one that
    created the current incarnation, competes per cell: the larger timestamp
    stays (§8.2).
  - A change made at an older incarnation changes no cell.
- **`coven_lost`**, for each value `a` of each cell of `r`:
  - `w`'s own value:
    - made at an incarnation already deleted: lost, replaced by that
      incarnation's generation record;
    - made at the current incarnation with a smaller timestamp than the
      cell's value: lost, replaced by the cell's value's write;
    - otherwise not lost.
  - a value already lost, which `w` replaces:
    - if `w` had read it: no longer lost;
    - otherwise still lost, and "replaced by" becomes the earlier delete or
      the later setter, by the rule in A5.
  - the cell's current value, which `w` replaces without having read it:
    lost, replaced by `w`.
  - anything else: unchanged.
- The step reads only:
  - the device's state;
  - the timestamps of writes it has applied, which `coven_writes` holds;
  - the arriving write's own record: its changes and its had-read positions.

## A7. Proof of the core

### A7.1 Structure

- Define `IsSpec S st`: state `st` is the result of A5 for set `S`. It is a
  list of "if and only if" statements, one per part of the state, that
  mention only `S` and the writes' metadata.
- **Step lemma**, Lean `step_spec`: if `S` is closed, `st` is the result for `S`,
  `w` is not in `S`, and everything `w` had read is in `S`, then applying `w`
  to `st` gives the result for `S` plus `w`.
- **Induction**, Lean `foldl_isSpec`: applying a causal order write by write
  keeps the invariant, from the empty database or from a snapshot.
- **Uniqueness**, Lean `isSpec_unique`: two states that are both the result for
  one set are equal, part by part.
- Convergence follows: both orders end in the result for the same set, and
  there is only one.
- Newman's lemma is not needed for the core: the core has no chained rules.
  Order independence is one theorem about the set, instead of a check of
  each pair of rules.

### A7.2 Facts the step lemma uses

- **Nothing applied had read the arriving write.** Every write in `S` had
  read only writes in `S`, since `S` is closed, and `w` is not in `S`. So any
  replacer already applied hadn't read `w`.
- **A change is never ahead of the device.** `g ≤ G`: by assumption 3, `g` is 0 or
  some write `w` had read moved the row to `g`, and that write is in `S`.
- **Generations go up one at a time.** If the row's generation is `G`, every
  generation from 1 to `G` was reached by some write in `S`. With assumption 4, an odd
  incarnation `k` was deleted exactly when `k < G`, and the generation
  record for `k + 1` is its earliest delete.
- **A value nobody replaced is the cell's current value**, in the current
  incarnation. Lean: `unreplaced`.
- **Every set value has a "replaced by" write** to name. Lean: `canon_exists`.

### A7.3 The cases

- Generation and generation record: the largest and smallest of a set, and
  adding `w` to the set changes them as the step does. Lean: `isMaxBy_insert`,
  `isMinBy_insert`.
- Cells: three cases — the row moves to a new generation, `w` sets the cell
  in the current incarnation, or neither.
- `coven_lost`, for each value `a`:
  - `a` is `w`'s own value: three cases by `w`'s incarnation against `G`.
  - `a` is already lost:
    - `w` replaces it having read it: no longer lost;
    - `w` replaces it without having read it: still lost; four cases for
      "replaced by", by whether `w` is a delete and whether `a`'s
      incarnation is deleted;
    - `w` doesn't replace it: unchanged, including when `w` sets the same
      cell with a smaller timestamp than `a`.
  - `a` is not lost: if some write in `S` replaced it, one of them had read
    it, and it stays not lost; otherwise `a` is the current value, and it
    becomes lost exactly when `w` replaces it without having read it.

## A8. Counterexamples to the spec as written

Each is two causal orders of the same writes that end differently. All are
checked in Lean by running an executable reading of the spec's rules
in `Counterexamples.lean`, namespace `Literal`. A12 lists how it reads the spec.

### A8.1 `coven_lost` depends on arrival order (§8.1, §8.2)

- The writes of A5's example.
- Ben's phone applies Ana 1, Ana 4, Ben 9, Carol 2:
  - Ben 9 replaces Ana 4 having read it: nothing recorded;
  - Carol 2 replaces Ben 9 without having read it: records "Weekly
    groceries".
- Carol's tablet applies Ana 1, Carol 2, Ana 4, Ben 9:
  - Ana 4 loses to Carol 2, which hadn't read it: records "Groceries";
  - Ben 9 loses to Carol 2: records "Weekly groceries".
- Ben's phone holds one `coven_lost` row, Carol's tablet two. The spec shows
  one and says every device holds the same rows.
- The cause: Ben 9 had read Ana 4 and replaces it, but arrives after Ana 4
  was recorded, and nothing takes the row back out.
- Fix: A5's rule. A write removes the lost rows of values it replaces and
  had read. Lean: `ce1_bens_phone`, `ce1_carols_tablet`.

### A8.2 `coven_rows.write` depends on arrival order (§8.3)

- Note 43 at generation 1; Ana's write 5 and Ben's write 6 delete it
  concurrently, stamped 50 and 60.
- Applied 5 then 6: `coven_rows` names write 5, since the second delete
  changes nothing. Applied 6 then 5: it names write 6.
- Fix: the generation record is the write with the smallest timestamp among
  those that moved the row to that generation. Same for concurrent re-adds.
  Lean: `ce2_gen_write`.

### A8.3 "Replaced by" after a re-add (§8.3)

- §8.3's example: Ana's write 5 creates note 43, her write 7 deletes it, her
  write 8 re-adds it; Ben's concurrent edit at generation 1 is stamped after
  17:00.
- The spec says every device records Ben's value as replaced by Ana's write
  7.
- A device that gets Ben's edit after Ana's write 8 has only write 8 in
  `coven_rows`, which keeps the latest generation change. It records write 8.
  A device that gets Ben's edit before Ana's write 7 records write 7.
- Fix: keep the write for every generation change of a row, one small row
  each. Lean: `ce3_replaced_by`.

### A8.4 Cascade for a child the deleter never had (§8.4)

- Tags point at notes with `ON DELETE CASCADE`.
- Writes:
  - Ana 1: insert notes 43 and 44;
  - Ana 2, had read Ana 1: insert tag 9 on note 43;
  - Ben 3, had read Ana 1, Ana 2: move tag 9 to note 44;
  - Carol 4, had read Ana 1 only: delete note 43.
- Ben's phone, 1 2 3 4: tag 9 is on note 44 when Carol's delete arrives, and
  stays.
- Carol's tablet, 1 4 2 3: tag 9 arrives pointing at a deleted note 43, so it
  is removed, its generation moved to 2. Ben's move, made at generation 1,
  then loses.
- Ben's phone has tag 9; Carol's tablet doesn't.
- The cause: §8.4's three cases consider only the child's insert, not a
  concurrent write that points the child elsewhere. Restrict gives the same
  result. Lean: `ce4_cascade`.

### A8.5 Set null for a child the deleter never had (§8.4)

- The same writes with link 6 through an `ON DELETE SET NULL` key.
- Ben's phone, 1 2 3 4: link 6 points at note 44.
- Carol's tablet, 1 4 2 3: link 6 arrives dangling, and coven sets it to null
  "set by the parent's delete", stamped 4. Ben's move, stamped 3, loses to
  that null.
- `ON UPDATE CASCADE` for a key change (§8.5) has the same shape: coven's
  re-pointing is stamped with the key change and competes with concurrent
  re-pointing by the child's own writes. Lean: `ce5_set_null`; the key
  change case is argued only here.

### A8.6 A unique loser is removed for good (§8.5)

- Note names are unique.
- Writes:
  - Ana 10: insert note 45, name "Groceries";
  - Ben 11: insert note 46, name "Groceries";
  - Ben 12, had read Ben 11 only: rename note 46 "Groceries 2".
- Ana's phone, 10 11 12: note 46 loses the name and is removed, generation 2.
  Ben's rename, made at generation 1, loses.
- Ben's phone, 11 12 10: no two notes share a name when Ana's arrives. Both
  stay. Lean: `ce6_unique`.

### A8.7 A CHECK loser is removed for good (§8.6)

- §8.6's example, `start <= end`, start 5, end 12. Ana sets start 10; Ben
  sets end 8, then end 20 before seeing Ana's write.
- Applied Ana, Ben 8, Ben 20: the row fails after Ben 8 and is removed; Ben
  20 loses.
- Applied Ben 8, Ben 20, Ana: start 10, end 20 passes; the row stays. Lean:
  `ce7_check`.

### A8.8 Unique and cascade chained (§8)

- Writes:
  - Ana 1: insert folder 70;
  - Ana 10: insert note 45 in folder 70, name "Groceries";
  - Ben 11: insert note 46, name "Groceries";
  - Carol 12: delete folder 70.
- Ana's phone, 1 10 11 12: note 46 loses the name to note 45; then the folder
  goes and note 45 cascades. Neither note remains.
- Carol's tablet, 1 12 10 11: note 45 arrives in a deleted folder and is
  removed; note 46 has no rival and stays.
- This one fails even within a single device's rule runs: from the state
  "folder deleted, notes 45 and 46 present", running cascade first leaves
  note 46, running unique first removes both. Lean: `ce8_unique_cascade`,
  and `spec_unique_not_confluent` for the rule-order version.

### A8.9 Unique values across circles (§14)

- Argued here only, not in Lean.
- A unique constraint on a table whose rows can be in different circles:
  Ana's row A in her circle and Ben's store row B claim one value, A's write
  earlier.
- Ana's device sees both and removes B. A device outside Ana's circle sees
  only B and keeps it. They disagree on a store row.
- Shared keys have the same problem: one key inserted concurrently in two
  places is one row on a device that reads both, and two different things
  elsewhere.
- Fix options: scope unique constraints and shared keys to one place, for
  example by including the place column, or refuse them on tables whose rows
  can be in different places.

### A8.10 Ancestor retraction for places

- The rule under test: an ancestor row has no place; it is shared when some
  shared child references it. When Ana's last shared child of ancestor 80
  leaves the store, her write retracts 80, a delete on other devices.
  Receivers ignore that delete while any present row references 80. Ben's
  write adding a new shared child carries 80 too.
- Writes:
  - write 1: insert ancestor 80 and child 81 pointing at it;
  - Ana 10: child 81 leaves the store, so delete 81 and retract 80;
  - Ben 11, concurrent: insert child 82 pointing at 80, carrying 80 at
    generation 1.
- Applied 1 10 11: when the retraction arrives no present row references 80,
  so 80 goes to generation 2. Ben's carried 80, made at generation 1, loses.
  Child 82 points at generation 1 of a row now at 2, so it follows its
  foreign key's action and is removed.
- Applied 1 11 10: child 82 references 80 when the retraction arrives, so it
  is ignored. 80 and 82 stay.
- The cause: whether the retraction applies depends on whether Ben's child
  has arrived, but once applied it moves the generation, which can't be
  undone. Lean: `ce9_ancestor`.
- Answer to the design question: an ancestor's presence should be derived
  from the merged children, not stored as a delete. "An ancestor is removed
  when every row referencing it is removed" is a rule of the kind in A9, so
  it converges with the foreign key rules, in any order. Ana's write then
  carries no delete of 80; every device computes 80's presence.

## A9. Post-rules: the general structure

### A9.1 Two requirements

- **Not stored.** A post-rule's result must not move a row's generation or
  be stored like a write. It must be recomputed from the merged state of A5
  whenever that state changes, and a removed row must come back when the
  reason goes away. A8.4–A8.7 and A8.10 fail this: each removal depends on
  which concurrent writes have arrived, and storing it makes it permanent.
- **One result from one merged state.** Running the rules in different
  orders must end in the same set of removed rows. A8.8 fails this.

### A9.2 Monotone rules have one result

- Model: a set `D` of removed rows; a rule `fires D x` says row `x` must be
  removed given `D`. One step removes some present row for which a rule
  fires.
- **Monotone**: if a rule fires for `x` given `D`, it also fires for `x`
  given any larger set. Removing more rows never stops a rule from firing.
- Theorems, for monotone rules over a finite set of rows:
  - **Termination**: each step removes a row, so the steps stop, as §8
    says. Lean: `killStep_terminating`.
  - **Local confluence**: two steps from one state, removing `x` and `y`,
    meet again: after removing `x`, the rule for `y` still fires, by
    monotonicity, and the reverse; both paths end with `x` and `y` removed.
    This is the only pair to check, whatever the rules are. Lean:
    `killStep_locallyConfluent`.
  - **Newman's lemma**: a terminating, locally confluent system has one end
    result from each state. Lean: `newman`, `kill_unique_normal`.
  - **Least fixpoint**: that end result is the smallest set containing the
    starting removals that is closed under the rules. Lean: `normal_least`. It
    can be stated without any order of application.
- So no list of rule pairs is needed: checking that each rule is monotone
  covers every pair, including ones not yet thought of.

### A9.3 Which rules are monotone

Judged against the merged state of A5, and the set `D`:

- **Cascade and restrict**: a child is removed when its winning reference
  names a parent generation the parent has left, or its parent is in `D`.
  Monotone.
- **CHECK**: a row is removed when its merged values fail. Doesn't depend on
  `D`. Monotone.
- **Ancestor**: an ancestor is removed when every row referencing it is in
  `D`. Monotone.
- Lean: `monoFires_monotone`.
- **Unique, as the spec states it**: a row is removed when a row still
  present claims its value with a smaller timestamp. "Still present" means
  "not in `D`": removing a row can stop the rule from firing for another.
  Not monotone, and A8.8 shows two end results.
- Worse, with unique values and foreign keys in a cycle there can be no
  fixpoint at all, so the result can't be stated without an order. Rows A,
  P, Q: A is P's child and Q is A's child, both by cascade; P and Q claim one
  value, Q's claim earlier. In a fixpoint, A is removed exactly when P is, P
  exactly when Q is present, and Q exactly when A is removed. So A is removed
  exactly when A is present.
- **Set null, set default, `ON UPDATE CASCADE`**: these change a reference,
  not a row's presence. They converge when the reference the app sees is
  computed from `D` and the merged reference, for example "the reference
  reads null while its parent is removed or its parent generation is stale".
  They must not feed the CHECK or unique rules, since nulling a column can
  make a CHECK pass or fail.

### A9.4 A rule set with one result

- Lean defines one, `Stratified`:
  1. run the monotone rules of A9.3 to the end;
  2. remove each row whose unique value is claimed, with a smaller
     timestamp, by a row still present after step 1. Judge this once, all at
     once;
  3. run the monotone rules to the end again, removing the children and
     unsupported ancestors of step 2's losers.
- It has exactly one result, and always has one. Lean: `stratified_unique`,
  `stratified_exists`.
- Freed values in step 3 don't re-open step 2. In A8.8, step 1 removes note
  45 in the deleted folder, and note 46 keeps the name on every device.
- **End to end**, Lean `end_to_end`: the post-rules read any function of the core
  state; since the core state is a function of the set of writes, so is the
  set of removed rows.
- Other rule sets work too, as long as each layer is monotone given the
  layers before it.
- This changes a claim of §8: "the result is the one applying every write in
  timestamp order would give" holds for the core but not for post-rules.
  In A8.8, timestamp order removes both notes; the stratified rules keep note
  46. Timestamp order with permanent removals is a function of the set too,
  but a late write with an old timestamp would require replaying everything
  after it, back to the oldest write ever, since §15 lets old writes arrive a
  year late.

## A10. Places and the rest of §8

- **Moving a row between places**: a delete for other devices, a re-add on
  return (§14). For the moved row this is its own change, covered by the
  core. For children another device added concurrently, it is the cascade of
  A8.4, with the same requirement: derived, not stored.
- **Circles** (§14.1): a device that can't read a circle's part skips it. In
  the core, a row's state depends only on changes to that row, which all
  sit in its circle's part, so devices agree on every row they can both read.
  Foreign keys keep this through §14.2. Unique values don't; see A8.9.
- **Key changes** (§8.5): a key change is a delete plus an insert, covered by
  the core. Which new key a child follows must be a function of the set: the
  spec picks the change with the larger timestamp, which is one. The
  re-pointing itself is the case of A8.5.
- **Triggers** (§8.7): a shared trigger's writes are ordinary writes, covered
  by the core. A local trigger sees every SQL change coven makes; its local
  table converges only if it computes a function of the current rows, such
  as an index or a count of rows, and not of the history of changes.

## A11. What Lean checks, and what is prose

Checked by Lean, with no `sorry`, no `admit` and no axioms beyond Lean's
built-in `propext`, `Classical.choice` and `Quot.sound`:

- `merge_converges`, `snapshot_converges`, `run_isSpec`, `isSpec_unique`,
  `causalOrder_of_ts_sorted`: A4, for the model of A3 and the result of A5.
- `newman`, `kill_unique_normal`, `kill_exists_normal`, `normal_least`:
  A9.2, for any monotone rules.
- `monoFires_monotone`: the rules of A9.3, stated over abstract inputs —
  which rows fail CHECK, each row's parent and whether its reference is
  stale, which rows are ancestors and who references them.
- `spec_unique_not_confluent`: two end results of the spec's rules, A8.8.
- `stratified_unique`, `stratified_exists`, `end_to_end`: A9.4.
- `ce1_…` to `ce9_…`: the counterexamples of A8.1–A8.8 and A8.10, by evaluating
  the executable reading of the spec.
- `example_8_1`: the proven step on §8.1's writes, in both orders.

Argued here only:

- That `Literal` reads the spec as written; A12 lists its choices.
- That the post-rule inputs — CHECK results, parents, stale references,
  unique claims, ancestor references — are functions of the core state.
  `end_to_end` holds for any such function; the reasoning that coven's
  actual inputs are such functions is prose.
- Set null, set default and `ON UPDATE CASCADE` as derived references
  (A9.3); key changes and concurrent renames (A10).
- Unique values and shared keys across circles (A8.9); circles generally
  (A10).
- That a device can carry out the step: the step reads only the state,
  applied writes' timestamps and the arriving write's record, which a reader
  of `Model.lean` can check, but no theorem states it.
- Local triggers (A10). Schema changes (§17), files and the writes "marked
  lost" of §17 are not modeled.

## A12. Interpretations of the spec

- "Had read" includes the device's own earlier writes.
- Every device applies a write after everything it had read, so the
  applied set is closed. Snapshots cover closed sets (§15).
- "Replaced by" names the earliest delete of the value's incarnation if it
  was deleted, otherwise the cell's current winner. The spec doesn't say
  which write when several replaced a value.
- An insert arriving at a row whose generation is one past the insert's is a
  concurrent re-add, and its cells merge (§8.3).
- In `Literal`, the executable reading used for A8:
  - a lost row is deleted like any delete: its generation moves on, and its
    coven_rows row names the parent's delete, the winning write, or the
    failing CHECK, as §8 says: "a lost row is deleted, like any delete";
  - post-rules run after each write, one violation at a time, until none is
    left; in every counterexample only one rule can fire at each point, so
    rule order doesn't matter there;
  - set null is a cell value set by the parent's delete, with its
    timestamp, as §8.4 says: "coven records the null as set by the parent's
    delete";
  - unique values are compared among rows present at the time (§8.5);
  - the ancestor carried in Ben's write is an update at its current
    generation.

## A13. Building

```
cd plans/proofs/merge
lake build                            # builds everything; no sorry
lake env lean CovenMerge/Axioms.lean  # prints each main result's axioms
```

- Toolchain: `leanprover/lean4:v4.34.1`, core Lean only, no Mathlib.
- Files:
  - `Model.lean`: writes, assumptions, the result of A5, the state, the step;
  - `Lemmas.lean`, `Step.lean`, `StepLost.lean`: the step lemma;
  - `Converge.lean`: A4;
  - `Rewriting.lean`: Newman's lemma;
  - `PostRules.lean`: A9;
  - `Counterexamples.lean`: A8 and `example_8_1`;
  - `Axioms.lean`: the axiom report.
